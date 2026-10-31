package org.neutron.ota
import android.util.Base64
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class NeutronOTAModule(private val context: ReactApplicationContext) : NativeNeutronOTASpec(context) {
    private val launchEpoch = NeutronOTAStore.shared?.launchEpoch
    private val executor = ThreadPoolExecutor(1,1,0L,TimeUnit.MILLISECONDS,ArrayBlockingQueue(8))
    override fun getName() = "NeutronOTA"
    override fun getTypedExportedConstants(): Map<String, Any> {
        val s = NeutronOTAStore.shared
        return mapOf("runtimeVersion" to (s?.runtimeVersion ?: ""), "appVersion" to (s?.appVersion ?: ""), "nativeBootTracking" to (s?.bootSelected == true))
    }
    private fun run(promise: Promise, work: (NeutronOTAStore) -> Any?) {
        try { executor.execute { try { val s = requireNotNull(NeutronOTAStore.shared); require(s.bootSelected && s.launchEpoch == launchEpoch); promise.resolve(work(s)) } catch (e: Exception) { promise.reject("OTA_ERROR", e) } } }
        catch (e: java.util.concurrent.RejectedExecutionException) { promise.reject("OTA_BUSY", e) }
    }
    private fun bytes(text: String): ByteArray { require(text.length <= 89478488); return Base64.decode(text, Base64.DEFAULT) }
    override fun readBootState(p: Promise) = run(p) { it.stateJSON() }
    override fun markHealthy(p: Promise) = run(p) { it.healthy() }
    override fun recordCrash(p: Promise) = run(p) { it.crash() }
    override fun rollback(p: Promise) = acceptReload(null, p)
    override fun verifyManifest(data: String, signature: String, key: String, p: Promise) = run(p) { it.verify(data, signature, key) }
    override fun sha256(data: String, p: Promise) = run(p) { java.security.MessageDigest.getInstance("SHA-256").digest(bytes(data)).joinToString("") { byte -> "%02x".format(byte.toInt() and 255) } }
    override fun beginStage(json: String, canonical: String, p: Promise) = run(p) { it.begin(json, canonical); null }
    override fun stageChunk(id: String, path: String, data: String, p: Promise) = run(p) { it.chunk(id,path,bytes(data)); null }
    override fun deleteStagedPath(id: String, path: String, p: Promise) = run(p) { it.delete(id,path); null }
    override fun stagedBundleHash(id: String, p: Promise) = run(p) { it.stagedHash(id) }
    override fun publishPending(json: String, canonical: String, p: Promise) = run(p) { it.commit(json,canonical) }
    override fun discardStage(id: String, p: Promise) = run(p) { it.discard(id); null }
    override fun reload(id: String, p: Promise) = acceptReload(id, p)
    private fun acceptReload(id: String?, p: Promise) {
        try { executor.execute {
            try {
                val store = requireNotNull(NeutronOTAStore.shared); require(store.bootSelected && store.launchEpoch == launchEpoch)
                val activity = requireNotNull(context.currentActivity) { "No native activity for reload" }
                val owner = store.reloadOwner
                var result: String? = null
                fun select() { if (id == null) { result = store.rollback(); store.prepareCurrentReload() } else store.prepareReload(id) }
                if (owner != null) {
                    select()
                    val path = store.selectBundleForBoot()
                    activity.runOnUiThread { owner(path) { failure -> if (failure == null) p.resolve(result) else p.reject("OTA_RELOAD", failure) } }
                } else {
                    val app = context.applicationContext as? com.facebook.react.ReactApplication ?: error("Host must implement ReactApplication")
                    require(context.hasActiveReactInstance() && app.reactNativeHost.hasInstance()) { "Configure the bridgeless reload owner" }
                    select()
                    activity.runOnUiThread {
                        try { app.reactNativeHost.clear(); activity.recreate(); p.resolve(result) }
                        catch (e: Exception) { p.reject("OTA_RELOAD", e) }
                    }
                }
            } catch (e: Exception) { p.reject("OTA_RELOAD", e) }
        } } catch (e: java.util.concurrent.RejectedExecutionException) { p.reject("OTA_BUSY", e) }
    }
    override fun invalidate() { executor.shutdown(); super.invalidate() }
}
