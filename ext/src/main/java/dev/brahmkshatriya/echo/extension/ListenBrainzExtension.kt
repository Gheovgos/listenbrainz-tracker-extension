package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.clients.LoginClient
import dev.brahmkshatriya.echo.common.clients.TrackerMarkClient
import dev.brahmkshatriya.echo.common.models.TrackDetails
import dev.brahmkshatriya.echo.common.models.User
import dev.brahmkshatriya.echo.common.settings.Setting
import dev.brahmkshatriya.echo.common.settings.SettingSwitch
import dev.brahmkshatriya.echo.common.settings.SettingTextInput
import dev.brahmkshatriya.echo.common.settings.Settings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

private fun lbLog(msg: String) = println("LbExt: $msg")

class ListenBrainzExtension : TrackerMarkClient, LoginClient.CustomInput {

    companion object {
        const val KEY_API_URL = "api_url"
        const val KEY_NOW_PLAYING = "now_playing"
        const val DEFAULT_API_URL = "https://api.listenbrainz.org"

        const val KEY_MIN_SECONDS = "min_seconds"
    }

    private lateinit var settings: Settings
    private val http = OkHttpClient()
    private val json = Json { ignoreUnknownKeys = true }

    private var currentUser: User? = null
    private var token: String? = null

    private var currentKey: String? = null
    private var startedAtSec = 0L
    private var wasPlaying = false
    private var nowPlayingKey: String? = null

    // --- Settings ---

    override suspend fun getSettingItems(): List<Setting> = listOf(
        SettingSwitch("Send \"now playing\"", KEY_NOW_PLAYING, "Show the current track on your ListenBrainz profile", true),
        SettingTextInput("API URL", KEY_API_URL, "Change only for compatible servers (self-hosted, Libre.fm...)", DEFAULT_API_URL),
        SettingTextInput(
            "Minimum listening time (seconds)", KEY_MIN_SECONDS,
            "Nothing is sent to ListenBrainz until the track has played this long (0 = no extra limit)", "0"
        )
    )

    override fun setSettings(settings: Settings) { this.settings = settings }

    private fun apiRoot() = settings.getString(KEY_API_URL)?.trim()?.trimEnd('/')?.ifEmpty { null } ?: DEFAULT_API_URL

    private fun minPlayMs(): Long =
        (settings.getString(KEY_MIN_SECONDS)?.trim()?.toIntOrNull() ?: 0).coerceIn(0, 600) * 1000L
    private fun nowPlayingEnabled() = settings.getBoolean(KEY_NOW_PLAYING) ?: true

    // --- Login ---

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
        if (details == null) { currentKey = null; wasPlaying = false; return }
        ensureTrack(details)
    }

    override suspend fun onPlayingStateChanged(details: TrackDetails?, isPlaying: Boolean) {
        if (details == null) { wasPlaying = false; return }
        ensureTrack(details)
        val k = keyOf(details)
        val unlocked = details.currentPosition >= minPlayMs()
        if (isPlaying && unlocked && (!wasPlaying || nowPlayingKey != k) && nowPlayingEnabled()) {
            nowPlayingKey = k
            submit("playing_now", details, null)
        }
        wasPlaying = isPlaying
    }

    override suspend fun getMarkAsPlayedDuration(details: TrackDetails): Long? {
        val total = details.totalDuration ?: details.track.duration
        if (total != null && total < 30_000) return null
        val rule = minOf(total?.div(2) ?: 120_000L, 240_000L)
        return minPlayMs().takeIf { it > 0 } ?: rule
    }

    override suspend fun onMarkAsPlayed(details: TrackDetails) {
        ensureTrack(details)
        submit("single", details, startedAtSec)
    }

    // --- Sending to ListenBrainz ---

    private fun trackMetadata(d: TrackDetails): JsonObject? {
        val t = d.track
        val artist = t.artists.joinToString(", ") { it.name }.trim()
        if (artist.isEmpty() || t.title.isBlank()) return null // artist_name & track_name
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

    private suspend fun submit(type: String, d: TrackDetails, listenedAt: Long?) {
        val tk = token ?: return
        val meta = trackMetadata(d) ?: run {
            lbLog("Skipped: ${d.track.title}")
            return
        }
        val body = buildJsonObject {
            put("listen_type", type)
            putJsonArray("payload") {
                add(buildJsonObject {
                    listenedAt?.let { put("listened_at", it) }
                    put("track_metadata", meta)
                })
            }
        }
        val req = Request.Builder().url(apiRoot() + "/1/submit-listens")
            .header("Authorization", "Token $tk")
            .post(body.toString().toRequestBody("application/json".toMediaType()))
            .build()
        try {
            withContext(Dispatchers.IO) {
                http.newCall(req).execute().use { res ->
                    if (!res.isSuccessful) lbLog("$type failed: HTTP ${res.code} ${res.body.string()}")
                    else lbLog("$type sent: ${d.track.title}")
                }
            }
        } catch (e: Exception) {
            lbLog("$type error: ${e.message}")
        }
    }
}