package org.neutron.ota

import android.content.Context
import android.system.Os
import android.system.OsConstants
import android.util.Base64
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.security.MessageDigest
import com.google.crypto.tink.subtle.Ed25519Verify

/** App-private, serialized store. Configure and select BEFORE constructing RN. */
class NeutronOTAStore(
    context: Context, private val packaged: File, publicKey: String,
    private val channel: String, val runtimeVersion: String, val appVersion: String
) : java.io.Closeable {
    companion object { @Volatile var shared: NeutronOTAStore? = null; const val LIMIT = 64 * 1024 * 1024 }
    private val root = File(context.noBackupFilesDir.canonicalFile, "neutron-ota")
    private val keyBytes = decodeKey(publicKey)
    private val verificationKey = Ed25519Verify(keyBytes.copyOfRange(12, 44))
    private val admitted = mutableMapOf<String, JSONObject>()
    private val stateFile get() = File(root, "state.json")
    /** Host owns replacing a bridgeless ReactHost whose loader is immutable. */
    var reloadOwner: ((String, (Throwable?) -> Unit) -> Unit)? = null
    var launchEpoch = java.util.UUID.randomUUID().toString(); private set
    var bootSelected = false; private set
    private val lockFile: java.io.RandomAccessFile
    private val processLock: java.nio.channels.FileLock
    init {
        privateDir(root); val lockPath = File(root, ".owner.lock"); noLinks(lockPath)
        lockFile = java.io.RandomAccessFile(lockPath, "rw"); Os.chmod(lockPath.path, 384)
        processLock = requireNotNull(lockFile.channel.tryLock()) { "OTA store already owned by another process" }
        try { if (!stateFile.exists()) save(empty()); read() } catch (e: Exception) { processLock.release(); lockFile.close(); throw e }
    }
    override fun close() { if (processLock.isValid) processLock.release(); lockFile.close() }
    private fun empty() = JSONObject().put("currentUpdateId", JSONObject.NULL).put("lastGoodUpdateId", JSONObject.NULL)
        .put("pendingUpdateId", JSONObject.NULL).put("buildNumber", 0L).put("consecutiveCrashes", 0).put("launchPending", false)
    private fun id(value: String): String { require(value.matches(Regex("[A-Za-z0-9_-]{1,128}"))) { "Invalid OTA identity" }; return value }
    private fun decodeKey(text: String): ByteArray {
        val bytes = Base64.decode(text.replace("-----BEGIN PUBLIC KEY-----", "").replace("-----END PUBLIC KEY-----", "").replace(Regex("\\s"), ""), Base64.DEFAULT)
        val prefix = byteArrayOf(0x30,0x2a,0x30,0x05,0x06,0x03,0x2b,0x65,0x70,0x03,0x21,0x00)
        if (bytes.size == 32) return prefix + bytes
        require(bytes.size == 44 && bytes.copyOfRange(0,12).contentEquals(prefix)) { "Expected Ed25519 raw or SPKI key" }; return bytes
    }
    private fun noLinks(file: File) {
        var cursor: File? = file.absoluteFile
        while (cursor != null) {
            try { require(!OsConstants.S_ISLNK(Os.lstat(cursor.path).st_mode)) { "Linked OTA path" } }
            catch (e: android.system.ErrnoException) { if (e.errno != OsConstants.ENOENT) throw e }
            cursor = cursor.parentFile
        }
    }
    private fun syncDir(file: File) { val fd = Os.open(file.path, OsConstants.O_RDONLY or OsConstants.O_DIRECTORY or OsConstants.O_NOFOLLOW, 0); try { Os.fsync(fd) } finally { Os.close(fd) } }
    private fun privateDir(file: File) {
        noLinks(file)
        if (!file.exists()) { val parent = requireNotNull(file.parentFile); if (!parent.exists()) privateDir(parent); require(file.mkdir()) { "Stage mkdir failed" }; Os.chmod(file.path, 448); syncDir(parent) }
        require(file.isDirectory && Os.stat(file.path).st_uid == Os.getuid()) { "Directory not app-owned" }
        Os.chmod(file.path, 448)
    }
    private fun publish(file: File, data: ByteArray) {
        require(data.size <= LIMIT); privateDir(requireNotNull(file.parentFile)); noLinks(file)
        val temp = File(file.parentFile, ".${java.util.UUID.randomUUID()}.tmp")
        val fd = Os.open(temp.path, OsConstants.O_WRONLY or OsConstants.O_CREAT or OsConstants.O_EXCL or OsConstants.O_NOFOLLOW, 384)
        try {
            FileOutputStream(fd).use { stream -> stream.write(data); stream.flush(); stream.fd.sync() }
            Os.rename(temp.path, file.path); syncDir(requireNotNull(file.parentFile))
        } finally { if (temp.exists()) temp.delete() }
    }
    private fun read(): JSONObject {
        noLinks(stateFile); require(stateFile.length() <= 16384); val s = JSONObject(stateFile.readText())
        val build = s.getDouble("buildNumber"); require(build >= 0 && build <= 9007199254740991.0 && build == kotlin.math.floor(build))
        require(s.getInt("consecutiveCrashes") >= 0); s.getBoolean("launchPending")
        listOf("currentUpdateId", "lastGoodUpdateId", "pendingUpdateId").forEach { if (!s.isNull(it)) id(s.getString(it)) }; return s
    }
    private fun save(s: JSONObject) = publish(stateFile, s.toString().toByteArray(Charsets.UTF_8))
    private fun bundle(name: String) = File(File(root, "bundles"), id(name))
    private fun stage(name: String) = File(File(root, "stages"), id(name))
    private fun path(directory: File, relative: String): File {
        require(relative.toByteArray().size <= 1024 && !relative.contains('\\') && !relative.contains('\u0000') && relative.split('/').all { it.isNotEmpty() && it != "." && it != ".." }) { "Invalid OTA relative path" }
        return File(directory, relative).also { noLinks(it) }
    }
    private fun files(directory: File): List<File> {
        noLinks(directory); val result = mutableListOf<File>()
        requireNotNull(directory.listFiles()).forEach { file -> noLinks(file); if (file.isDirectory) result += files(file) else { require(file.isFile); result += file }; require(result.size <= 4096) }
        return result
    }
    private fun hex(bytes: ByteArray) = bytes.joinToString("") { "%02x".format(it.toInt() and 255) }
    /** Same inventory digest as iOS: sorted UTF-8 path, NUL, file SHA256, LF. */
    fun digest(directory: File): String {
        val combined = MessageDigest.getInstance("SHA-256"); var total = 0L
        files(directory).sortedWith { a, b ->
            val left = a.relativeTo(directory).invariantSeparatorsPath.toByteArray(Charsets.UTF_8)
            val right = b.relativeTo(directory).invariantSeparatorsPath.toByteArray(Charsets.UTF_8)
            var comparison = 0
            for (i in 0 until minOf(left.size, right.size)) { comparison = (left[i].toInt() and 255) - (right[i].toInt() and 255); if (comparison != 0) break }
            if (comparison != 0) comparison else left.size - right.size
        }.forEach { file ->
            val hash = MessageDigest.getInstance("SHA-256")
            FileInputStream(file).use { input -> val buffer = ByteArray(65536); while (true) { val count = input.read(buffer); if (count < 0) break; total += count; require(total <= LIMIT); hash.update(buffer, 0, count) } }
            combined.update((file.relativeTo(directory).invariantSeparatorsPath + "\u0000" + hex(hash.digest()) + "\n").toByteArray(Charsets.UTF_8))
        }; return hex(combined.digest())
    }
    @Synchronized fun verify(canonical: String, signature: String, key: String): Boolean {
        if (!decodeKey(key).contentEquals(keyBytes)) return false
        val bytes = Base64.decode(signature, Base64.DEFAULT); if (bytes.size != 64) return false
        return try { verificationKey.verify(bytes, canonical.toByteArray(Charsets.UTF_8)); true } catch (_: java.security.GeneralSecurityException) { false }
    }
    private fun manifest(json: String, canonical: String, admission: Boolean = true): JSONObject {
        require(json.toByteArray().size <= 1048576 && canonical.toByteArray().size <= 1048576)
        val m = JSONObject(json); val signed = JSONObject(canonical)
        require(verify(canonical, m.getString("signature"), Base64.encodeToString(keyBytes, Base64.NO_WRAP))) { "Manifest signature invalid" }
        val fields = listOf("id", "version", "buildNumber", "runtimeVersion", "channel", "bundleHash", "downloadSize", "createdAt", "minAppVersion", "chunks")
        val normalized = JSONObject(); fields.forEach { normalized.put(it, m.opt(it) ?: JSONObject.NULL) }
        require(equalJSON(normalized, signed)) { "Manifest differs from signed fields" }
        id(m.getString("id")); require(m.getString("channel") == channel && m.getString("runtimeVersion") == runtimeVersion)
        val build = m.getDouble("buildNumber"); require(build == kotlin.math.floor(build) && build <= 9007199254740991.0 && (!admission || build > read().getDouble("buildNumber"))) { "Rollback build" }
        require(m.getString("bundleHash").matches(Regex("[a-f0-9]{64}")))
        val chunks = m.getJSONArray("chunks"); require(chunks.length() <= 4096); val paths = mutableSetOf<String>(); var total = 0L
        for (i in 0 until chunks.length()) { val c = chunks.getJSONObject(i); val p = c.getString("path"); path(root, p); require(paths.add(p)); val size = c.getLong("size"); require(size in 0..LIMIT.toLong()); when(c.getString("operation")) {
            "delete" -> require(size == 0L)
            "add", "modify" -> { require(c.getString("hash").matches(Regex("[a-f0-9]{64}"))); require(java.net.URI(c.getString("url")).scheme == "https"); total += size }
            else -> error("Invalid chunk operation")
        } }
        require(total == m.getLong("downloadSize") && total <= LIMIT)
        if (!m.isNull("minAppVersion")) {
            fun version(text: String): List<Int> { require(text.matches(Regex("[0-9]+\\.[0-9]+\\.[0-9]+"))); return text.split('.').map { it.toInt() } }
            val current = version(appVersion); val minimum = version(m.getString("minAppVersion"))
            val difference = (0..2).firstOrNull { current[it] != minimum[it] }
            require(difference == null || current[difference] > minimum[difference]) { "Newer native app required" }
        }; return m
    }
    private fun equalJSON(a: Any?, b: Any?): Boolean = when {
        a is JSONObject && b is JSONObject -> a.length() == b.length() && a.keys().asSequence().all { b.has(it) && equalJSON(a.get(it), b.get(it)) }
        a is org.json.JSONArray && b is org.json.JSONArray -> a.length() == b.length() && (0 until a.length()).all { equalJSON(a.get(it), b.get(it)) }
        a is Number && b is Number -> a.toDouble() == b.toDouble()
        else -> a == b
    }
    @Synchronized fun stateJSON() = read().toString()
    @Synchronized fun healthy(): String { require(bootSelected); val s = read(); s.put("lastGoodUpdateId", s.get("currentUpdateId")).put("launchPending", false).put("consecutiveCrashes", 0); save(s); return stateJSON() }
    @Synchronized fun crash(): String { val s = read(); s.put("consecutiveCrashes", s.getInt("consecutiveCrashes") + 1).put("launchPending", false); save(s); return stateJSON() }
    @Synchronized fun rollback(): String { val s = read(); s.put("currentUpdateId", s.get("lastGoodUpdateId")).put("pendingUpdateId", JSONObject.NULL).put("consecutiveCrashes", 0).put("launchPending", false); save(s); return stateJSON() }
    @Synchronized fun selectBundleForBoot(): String {
        if (bootSelected) return selected(read()).path
        val s = read(); if (s.getBoolean("launchPending")) s.put("consecutiveCrashes", s.getInt("consecutiveCrashes") + 1)
        if (s.getInt("consecutiveCrashes") >= 3) s.put("currentUpdateId", s.get("lastGoodUpdateId")).put("pendingUpdateId", JSONObject.NULL).put("consecutiveCrashes", 0)
        else if (!s.isNull("pendingUpdateId")) s.put("consecutiveCrashes", 0).put("currentUpdateId", s.get("pendingUpdateId")).put("pendingUpdateId", JSONObject.NULL)
        val selected = selected(s); s.put("launchPending", true); save(s); launchEpoch = java.util.UUID.randomUUID().toString(); bootSelected = true; return selected.path
    }
    private fun selected(s: JSONObject): File {
        val dir = if (s.isNull("currentUpdateId")) packaged else bundle(s.getString("currentUpdateId"))
        if (!s.isNull("currentUpdateId")) {
            val receiptFile = File(File(root, "receipts"), id(s.getString("currentUpdateId")) + ".json"); noLinks(receiptFile); require(receiptFile.length() <= 2097152)
            val receipt = JSONObject(receiptFile.readText()); val m = manifest(receipt.getString("manifest"), receipt.getString("canonical"), false)
            require(m.getString("id") == s.getString("currentUpdateId") && digest(dir) == m.getString("bundleHash")) { "Authenticated boot bundle digest differs" }
        }
        return path(dir, "index.jsbundle").also { require(it.isFile) { "Selected JS bundle missing" } }
    }
    @Synchronized fun begin(json: String, canonical: String) {
        require(admitted.isEmpty() && read().isNull("pendingUpdateId")) { "Another update owns staging or pending boot" }
        val m = manifest(json, canonical); val name = m.getString("id"); val dest = stage(name); require(!dest.exists()); privateDir(dest)
        val s = read(); val source = if (s.isNull("currentUpdateId")) packaged else bundle(s.getString("currentUpdateId"))
        digest(source)
        files(source).forEach { publish(path(dest, it.relativeTo(source).invariantSeparatorsPath), it.readBytes()) }; admitted[name] = m
    }
    @Synchronized fun chunk(name: String, relative: String, bytes: ByteArray) {
        val m = requireNotNull(admitted[name]); val chunks = m.getJSONArray("chunks"); val c = (0 until chunks.length()).map { chunks.getJSONObject(it) }.first { it.getString("path") == relative }
        require(c.getString("operation") != "delete" && bytes.size == c.getInt("size") && bytes.size <= LIMIT && hex(MessageDigest.getInstance("SHA-256").digest(bytes)) == c.getString("hash"))
        publish(path(stage(name), relative), bytes)
    }
    @Synchronized fun delete(name: String, relative: String) {
        val chunks = requireNotNull(admitted[name]).getJSONArray("chunks"); require((0 until chunks.length()).any { chunks.getJSONObject(it).let { it.getString("path") == relative && it.getString("operation") == "delete" } })
        val file = path(stage(name), relative); if (file.exists()) { require(file.isFile && file.delete()); syncDir(requireNotNull(file.parentFile)) }
    }
    @Synchronized fun stagedHash(name: String): String { require(admitted.containsKey(name)); return digest(stage(name)) }
    @Synchronized fun commit(json: String, canonical: String): String {
        val m = manifest(json, canonical); val name = m.getString("id"); require(equalJSON(requireNotNull(admitted[name]), m)); require(digest(stage(name)) == m.getString("bundleHash")); require(path(stage(name), "index.jsbundle").isFile)
        val dest = bundle(name); privateDir(requireNotNull(dest.parentFile)); require(!dest.exists()); Os.rename(stage(name).path, dest.path); syncDir(requireNotNull(dest.parentFile)); syncDir(File(root, "stages"))
        publish(File(File(root, "receipts"), name + ".json"), JSONObject().put("manifest", json).put("canonical", canonical).toString().toByteArray(Charsets.UTF_8))
        val s = read(); s.put("pendingUpdateId", name).put("buildNumber", m.getLong("buildNumber")); save(s); admitted.remove(name); return stateJSON()
    }
    @Synchronized fun discard(name: String) { val dir = stage(name); noLinks(dir); if (dir.exists()) { require(dir.deleteRecursively()); syncDir(requireNotNull(dir.parentFile)) }; admitted.remove(name) }
    @Synchronized fun prepareCurrentReload() { bootSelected = false; selectBundleForBoot() }
    @Synchronized fun prepareReload(name: String) { require(read().optString("pendingUpdateId") == name); bootSelected = false; selectBundleForBoot() }
}
