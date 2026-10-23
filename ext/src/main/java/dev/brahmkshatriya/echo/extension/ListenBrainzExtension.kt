package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.clients.LikeClient
import dev.brahmkshatriya.echo.common.clients.LoginClient
import dev.brahmkshatriya.echo.common.clients.TrackerMarkClient
import dev.brahmkshatriya.echo.common.models.EchoMediaItem
import dev.brahmkshatriya.echo.common.models.TrackDetails
import dev.brahmkshatriya.echo.common.models.User
import dev.brahmkshatriya.echo.common.settings.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.File
import okhttp3.HttpUrl.Companion.toHttpUrl

private fun lbLog(msg: String) = println("LbExt: $msg")

class ListenBrainzExtension : TrackerMarkClient, LoginClient.CustomInput, LikeClient {

    companion object {
        const val KEY_API_URL = "api_url"
        const val KEY_PLAYED = "played"
        const val KEY_NOW_PLAYING_DELAY = "now_playing_delay"
        const val KEY_MIN_SECONDS = "min_seconds"
        const val KEY_OFFLINE_QUEUE = "offline_queue"
        const val DEFAULT_API_URL = "https://api.listenbrainz.org"
        private const val MAX_QUEUE = 2000
        private const val BATCH = 100
        const val KEY_DEDUP_WINDOW = "dedup_window"

        const val KEY_AUTO_LOVE = "auto_love"

        const val KEY_PRIVATE = "private"

    }

    private enum class SendResult { OK, RETRY, DROP }

    private lateinit var settings: Settings
    private val http = OkHttpClient()
    private val json = Json { ignoreUnknownKeys = true }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private var currentUser: User? = null
    private var token: String? = null

    // current listening
    private var currentKey: String? = null
    private var startedAtSec = 0L
    private var wasPlaying = false
    private var nowPlayingKey: String? = null
    private var scheduledKey: String? = null
    private var nowPlayingJob: Job? = null

    // offline queue
    private val queueMutex = Mutex()
    private val pending = mutableListOf<JsonObject>()
    private var queueOwner: String? = null
    private val queueFile: File by lazy {
        val dir = File(System.getProperty("java.io.tmpdir") ?: ".", "listenbrainz_ext")
        dir.mkdirs()
        File(dir, "pending.json")
    }

    // --- Settings ---

    override suspend fun getSettingItems(): List<Setting> = listOf(
        SettingTextInput(
            "Now playing delay (seconds)", KEY_NOW_PLAYING_DELAY,
            "Show the track on your profile after this many seconds (0 = immediately)", "0"
        ),
        SettingSwitch("Send \"Played\"", KEY_PLAYED, "Show the current track marked as \"Listen\" on your ListenBrainz profile", true),
        SettingSwitch("Offline queue", KEY_OFFLINE_QUEUE, "Keep listens when offline and send them later", true),
        SettingTextInput(
            "Listen threshold (seconds)", KEY_MIN_SECONDS,
            "Submit the listen after this many seconds (0 = ListenBrainz rule: half the track, max 4 min)", "0"
        ),
        SettingTextInput(
            "Hide repeats within N tracks", KEY_DEDUP_WINDOW,
            "If the same track was played in the last N plays, don't submit it (0 = off)", "0"
        ),
        SettingSwitch(
            "Auto-love listened tracks", KEY_AUTO_LOVE,
            "Every track counted as listened is also marked as loved on ListenBrainz", false
        ),
        SettingSwitch(
            "Auto-private listened tracks", KEY_PRIVATE,
            "Every track counted as listened is marked as private on ListenBrainz", false
        ),
        SettingTextInput("API URL", KEY_API_URL, "Change only for compatible servers (self-hosted, Libre.fm...)", DEFAULT_API_URL),
    )

    override fun setSettings(settings: Settings) { this.settings = settings }

    private fun apiRoot() = settings.getString(KEY_API_URL)?.trim()?.trimEnd('/')?.ifEmpty { null } ?: DEFAULT_API_URL
    private fun nowPlayingEnabled() = settings.getBoolean(KEY_PLAYED) ?: true
    private fun offlineQueueEnabled() = settings.getBoolean(KEY_OFFLINE_QUEUE) ?: true
    private fun minPlayMs(): Long =
        (settings.getString(KEY_MIN_SECONDS)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 600) * 1000L

    private fun nowPlayingDelayMs(): Long =
        (settings.getString(KEY_NOW_PLAYING_DELAY)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 600) * 1000L

    private fun autoLoveEnabled() = settings.getBoolean(KEY_AUTO_LOVE) ?: false

    private fun autoPrivateEnabled() = settings.getBoolean(KEY_AUTO_LOVE) ?: false


    // --- Login---

    override val forms = listOf(
        LoginClient.Form(
            key = "lb_login",
            label = "ListenBrainz",
            icon = LoginClient.InputField.Type.Password,
            inputFields = listOf(
                LoginClient.InputField(LoginClient.InputField.Type.Password, "token", "User token (listenbrainz.org/settings)", true)
            )
        )
    )

    override suspend fun onLogin(key: String, data: Map<String, String?>): List<User> {
        val t = data["token"]?.trim().orEmpty()
        require(t.isNotEmpty()) { "Token required" }
        val req = Request.Builder().url(apiRoot() + "/1/validate-token")
            .header("Authorization", "Token $t").get().build()
        val body = withContext(Dispatchers.IO) {
            http.newCall(req).execute().use { it.body.string() }
        }
        val o = runCatching { json.parseToJsonElement(body).jsonObject }.getOrNull()
        if (o?.get("valid")?.jsonPrimitive?.booleanOrNull != true) error("Invalid ListenBrainz token")
        val name = o["user_name"]?.jsonPrimitive?.contentOrNull ?: "ListenBrainz"
        return listOf(User(id = name, name = name, subtitle = "ListenBrainz", extras = mapOf("token" to t)))
    }

    override fun setLoginUser(user: User?) {
        currentUser = user
        token = user?.extras?.get("token")
        if (token != null) scope.launch { flushQueue() }
    }

    override suspend fun getCurrentUser(): User? = currentUser?.copy(extras = emptyMap())

    // --- Tracker ---

    private fun keyOf(d: TrackDetails) = "${d.extensionId}:${d.track.id}"

    private fun ensureTrack(d: TrackDetails) {
        val k = keyOf(d)
        if (k != currentKey) {
            currentKey = k
            startedAtSec = System.currentTimeMillis() / 1000 - d.currentPosition / 1000
        }
    }

    override suspend fun onTrackChanged(details: TrackDetails?) {
        if (details == null) {
            currentKey = null
            wasPlaying = false
            nowPlayingJob?.cancel()
            scheduledKey = null
            return
        }
        ensureTrack(details)
        scope.launch { flushQueue() }
    }

    override suspend fun onPlayingStateChanged(details: TrackDetails?, isPlaying: Boolean) {
        if (details == null) {
            nowPlayingJob?.cancel()
            scheduledKey = null
            wasPlaying = false
            return
        }
        ensureTrack(details)
        val k = keyOf(details)
        if (!isPlaying) {
            nowPlayingJob?.cancel()
            scheduledKey = null
        } else if ((!wasPlaying || nowPlayingKey != k) && scheduledKey != k && nowPlayingEnabled()) {
            nowPlayingJob?.cancel()
            scheduledKey = k
            val wait = (nowPlayingDelayMs() - details.currentPosition).coerceAtLeast(0L)
            nowPlayingJob = scope.launch {
                if (wait > 0) delay(wait)
                nowPlayingKey = k
                sendNowPlaying(details)
            }
        }
        wasPlaying = isPlaying
    }

    override suspend fun getMarkAsPlayedDuration(details: TrackDetails): Long? {
        val total = details.totalDuration ?: details.track.duration
        val custom = minPlayMs()
        if (custom > 0) return if (total != null) custom.coerceAtMost(total * 9 / 10) else custom
        if (total != null && total < 30_000) return null
        return minOf(total?.div(2) ?: 120_000L, 240_000L)
    }

    override suspend fun onMarkAsPlayed(details: TrackDetails) {
        ensureTrack(details)
        if (isRepeat(details)) {
            lbLog("hiding repeat: ${details.track.title}")
            return
        }
        val meta = trackMetadata(details) ?: run {
            lbLog("skipped, missing data: ${details.track.title}")
            return
        }
        val listen = listenObject(meta, startedAtSec, true)
        if (offlineQueueEnabled()) {
            queueMutex.withLock {
                ensureQueueLoaded()
                pending.add(listen)
                while (pending.size > MAX_QUEUE) pending.removeAt(0)
                saveQueue()
            }
            flushQueue()
        } else {
            post("single", listOf(listen))
        }
        lbLog("onMarkAsPlayed: autolove=${autoLoveEnabled()} token=${token != null}")
        if (autoLoveEnabled()) scope.launch { autoLove(details) }
    }

    // --- Costruzione dei dati ---

    private fun trackMetadata(d: TrackDetails): JsonObject? {
        val t = d.track
        val artist = t.artists.joinToString(", ") { it.name }.trim()
        if (artist.isEmpty() || t.title.isBlank()) return null
        return buildJsonObject {
            put("artist_name", artist)
            put("track_name", t.title)
            t.album?.title?.takeIf { it.isNotBlank() }?.let { put("release_name", it) }
            t.extras["mbid"]?.takeIf { it.isNotBlank() }?.let { put("recording_mbid", it) }
            t.album?.extras?.get("mbid")?.takeIf { it.isNotBlank() }?.let { put("release_mbid", it) }
            putJsonObject("additional_info") {
                put("media_player", "Echo")
                put("submission_client", "Echo ListenBrainz extension")
                put("submission_client_version", "1.0")
                (d.totalDuration ?: t.duration)?.let { put("duration_ms", it) }
                t.isrc?.takeIf { it.isNotBlank() }?.let { put("isrc", it) }
            }
        }
    }

    private fun listenObject(meta: JsonObject, listenedAt: Long?, isNowPlaying: Boolean) = buildJsonObject {
        listenedAt?.let { put("listened_at", it) }
        put("track_metadata", meta)
        if (autoPrivateEnabled() && !isNowPlaying) put("private", true)
    }

    private suspend fun sendNowPlaying(d: TrackDetails) {
        val meta = trackMetadata(d) ?: return
        post("playing_now", listOf(listenObject(meta, null, false)))
    }

    private val recentPlays = ArrayDeque<String>()

    private fun dedupWindow() =
        (settings.getString(KEY_DEDUP_WINDOW)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 100)

    private fun isRepeat(d: TrackDetails): Boolean {
        val window = dedupWindow()
        val k = keyOf(d)
        synchronized(recentPlays) {
            val repeat = window > 0 && recentPlays.takeLast(window).contains(k)
            recentPlays.addLast(k)
            while (recentPlays.size > 100) recentPlays.removeFirst()
            return repeat
        }
    }

    // --- Offline ---
    private fun ensureQueueLoaded() {
        val owner = currentUser?.id
        if (owner != null && queueOwner == owner) return
        queueOwner = owner
        pending.clear()
        if (owner == null) return
        try {
            if (!queueFile.exists()) return
            val o = json.parseToJsonElement(queueFile.readText()).jsonObject
            if (o["owner"]?.jsonPrimitive?.contentOrNull != owner) return
            (o["listens"] as? JsonArray)?.forEach { (it as? JsonObject)?.let(pending::add) }
        } catch (e: Exception) {
            lbLog("queue not readable: ${e.message}")
        }
    }

    private fun saveQueue() {
        try {
            val owner = queueOwner ?: return
            if (pending.isEmpty()) { queueFile.delete(); return }
            queueFile.writeText(
                buildJsonObject {
                    put("owner", owner)
                    put("listens", JsonArray(pending))
                }.toString()
            )
        } catch (e: Exception) {
            lbLog("queue not saved: ${e.message}")
        }
    }

    private suspend fun flushQueue() {
        queueMutex.withLock {
            if (token == null) return@withLock
            ensureQueueLoaded()
            while (pending.isNotEmpty()) {
                val batch = pending.take(BATCH)
                val type = if (batch.size == 1) "single" else "import"
                when (post(type, batch)) {
                    SendResult.RETRY -> return@withLock
                    else -> {
                        repeat(batch.size) { pending.removeAt(0) }
                        saveQueue()
                    }
                }
            }
        }
    }
    override suspend fun likeItem(item: EchoMediaItem, shouldLike: Boolean) {
        lbLog("likeItem received: ${item.title} -> $shouldLike")
    }
    override suspend fun isItemLiked(item: EchoMediaItem): Boolean = false
    private val lovedMbids = mutableSetOf<String>()

    private suspend fun lookupMbid(d: TrackDetails): String? {
        d.track.extras["mbid"]?.takeIf { it.isNotBlank() }?.let { return it }

        val names = d.track.artists.map { it.name }.filter { it.isNotBlank() }
        val candidates = listOfNotNull(names.joinToString(", ").ifEmpty { null }, names.firstOrNull()).distinct()
        if (candidates.isEmpty() || d.track.title.isBlank()) return null

        for (artist in candidates) {
            val url = (apiRoot() + "/1/metadata/lookup/").toHttpUrl().newBuilder()
                .addQueryParameter("artist_name", artist)
                .addQueryParameter("recording_name", d.track.title)
                .build()
            val body = try {
                withContext(Dispatchers.IO) {
                    http.newCall(Request.Builder().url(url).header("Authorization", "Token ${token.orEmpty()}").get().build()).execute().use {
                        val b = if (it.isSuccessful) it.body.string() else null
                        lbLog("lookup HTTP ${it.code} per '$artist' - '${d.track.title}': ${b?.take(300)}")
                        b
                    }
                }
            } catch (e: Exception) {
                lbLog("lookup errore: ${e.message}")
                null
            } ?: continue
            val mbid = runCatching {
                json.parseToJsonElement(body).jsonObject["recording_mbid"]?.jsonPrimitive?.contentOrNull
            }.getOrNull()
            if (!mbid.isNullOrBlank()) return mbid
        }
        return null
    }

    private suspend fun autoLove(d: TrackDetails) {
        lbLog("autolove: ${d.track.artists.joinToString { it.name }} - ${d.track.title}")
        val tk = token ?: return
        val mbid = lookupMbid(d) ?: run {
            lbLog("love skipped (no MBID): ${d.track.title}")
            return
        }
        if (!synchronized(lovedMbids) { lovedMbids.add(mbid) }) return

        val body = buildJsonObject { put("recording_mbid", mbid); put("score", 1) }
        val req = Request.Builder().url(apiRoot() + "/1/feedback/recording-feedback")
            .header("Authorization", "Token $tk")
            .post(body.toString().toRequestBody("application/json".toMediaType()))
            .build()
        try {
            withContext(Dispatchers.IO) {
                http.newCall(req).execute().use { res ->
                    if (res.isSuccessful) lbLog("love sent: $mbid")
                    else { lbLog("love fallito: HTTP ${res.code}"); synchronized(lovedMbids) { lovedMbids.remove(mbid) } }
                }
            }
        } catch (e: Exception) {
            lbLog("love errore di rete: ${e.message}")
            synchronized(lovedMbids) { lovedMbids.remove(mbid) }
        }
    }

    // --- Requests ---

    private suspend fun post(type: String, listens: List<JsonObject>): SendResult {
        val tk = token ?: return SendResult.RETRY
        val body = buildJsonObject {
            put("listen_type", type)
            put("payload", JsonArray(listens))
        }
        val req = Request.Builder().url(apiRoot() + "/1/submit-listens")
            .header("Authorization", "Token $tk")
            .post(body.toString().toRequestBody("application/json".toMediaType()))
            .build()
        return try {
            withContext(Dispatchers.IO) {
                http.newCall(req).execute().use { res ->
                    when {
                        res.isSuccessful -> { lbLog("$type sent (${listens.size})"); SendResult.OK }
                        res.code == 401 || res.code == 429 || res.code >= 500 -> {
                            lbLog("$type da ritentare: HTTP ${res.code}"); SendResult.RETRY
                        }
                        else -> { lbLog("$type scartato: HTTP ${res.code} ${res.body.string()}"); SendResult.DROP }
                    }
                }
            }
        } catch (e: Exception) {
            lbLog("$type errore di rete: ${e.message}")
            SendResult.RETRY
        }
    }
}