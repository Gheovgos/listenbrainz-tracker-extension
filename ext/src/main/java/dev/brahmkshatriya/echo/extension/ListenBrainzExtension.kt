package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.clients.LoginClient
import dev.brahmkshatriya.echo.common.clients.TrackerMarkClient
import dev.brahmkshatriya.echo.common.models.TrackDetails
import dev.brahmkshatriya.echo.common.models.User
import dev.brahmkshatriya.echo.common.settings.Setting
import dev.brahmkshatriya.echo.common.settings.SettingSwitch
import dev.brahmkshatriya.echo.common.settings.SettingTextInput
import dev.brahmkshatriya.echo.common.settings.Settings
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

private fun lbLog(msg: String) = println("LbExt: $msg")

class ListenBrainzExtension : TrackerMarkClient, LoginClient.CustomInput {

    companion object {
        const val KEY_API_URL = "api_url"
        const val KEY_NOW_PLAYING = "now_playing"
        const val KEY_MIN_SECONDS = "min_seconds"
        const val KEY_OFFLINE_QUEUE = "offline_queue"
        const val DEFAULT_API_URL = "https://api.listenbrainz.org"
        private const val MAX_QUEUE = 2000
        private const val BATCH = 100
        const val KEY_DEDUP_WINDOW = "dedup_window"
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
        SettingSwitch("Send \"now playing\"", KEY_NOW_PLAYING, "Show the current track on your ListenBrainz profile", true),
        SettingSwitch("Offline queue", KEY_OFFLINE_QUEUE, "Keep listens when offline and send them later", true),
        SettingTextInput(
            "Listen threshold (seconds)", KEY_MIN_SECONDS,
            "Submit the listen after this many seconds (0 = ListenBrainz rule: half the track, max 4 min)", "0"
        ),
        SettingTextInput(
            "Hide repeats within N tracks", KEY_DEDUP_WINDOW,
            "If the same track was played in the last N plays, don't submit it (0 = off)", "0"
        ),
        SettingTextInput("API URL", KEY_API_URL, "Change only for compatible servers (self-hosted, Libre.fm...)", DEFAULT_API_URL)
    )

    override fun setSettings(settings: Settings) { this.settings = settings }

    private fun apiRoot() = settings.getString(KEY_API_URL)?.trim()?.trimEnd('/')?.ifEmpty { null } ?: DEFAULT_API_URL
    private fun nowPlayingEnabled() = settings.getBoolean(KEY_NOW_PLAYING) ?: true
    private fun offlineQueueEnabled() = settings.getBoolean(KEY_OFFLINE_QUEUE) ?: true
    private fun minPlayMs(): Long =
        (settings.getString(KEY_MIN_SECONDS)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 600) * 1000L

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
            val wait = (minPlayMs() - details.currentPosition).coerceAtLeast(0L)
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
        val listen = listenObject(meta, startedAtSec)
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
            putJsonObject("additional_info") {
                put("media_player", "Echo")
                put("submission_client", "Echo ListenBrainz extension")
                put("submission_client_version", "1.0")
                (d.totalDuration ?: t.duration)?.let { put("duration_ms", it) }
                t.isrc?.takeIf { it.isNotBlank() }?.let { put("isrc", it) }
            }
        }
    }

    private fun listenObject(meta: JsonObject, listenedAt: Long?) = buildJsonObject {
        listenedAt?.let { put("listened_at", it) }
        put("track_metadata", meta)
    }

    private suspend fun sendNowPlaying(d: TrackDetails) {
        val meta = trackMetadata(d) ?: return
        post("playing_now", listOf(listenObject(meta, null)))
    }

    private val recentPlays = ArrayDeque<String>()

    private fun dedupWindow() =
        (settings.getString(KEY_DEDUP_WINDOW)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 100)

    private fun identityOf(d: TrackDetails): String {
        fun n(s: String) = s.lowercase().filter { it.isLetterOrDigit() }
        return n(d.track.artists.joinToString(",") { it.name }) + "|" + n(d.track.title)
    }

    private fun isRepeat(d: TrackDetails): Boolean {
        val window = dedupWindow()
        val id = identityOf(d)
        synchronized(recentPlays) {
            val repeat = window > 0 && recentPlays.takeLast(window).contains(id)
            recentPlays.addLast(id)
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
                        res.isSuccessful -> { lbLog("$type inviato (${listens.size})"); SendResult.OK }
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