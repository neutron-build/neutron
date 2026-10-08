package org.neutron.ota
import android.test.AndroidTestCase
import org.json.JSONObject
import java.io.File
import android.util.Base64
/** Add to the acceptance app's androidTest source set; native storage, not a JS fixture. */
class NeutronOTAStoreTest : AndroidTestCase() {
    fun testSignedDeletionPublicationAndReopenCrashRollback() {
        val vector = JSONObject(context.assets.open("vector.json").bufferedReader().use { it.readText() })
        val packaged = File(context.filesDir, "ota-fixture-${System.nanoTime()}"); packaged.mkdirs()
        packaged.resolve("assets").mkdir(); context.assets.open("packaged/index.jsbundle").use { packaged.resolve("index.jsbundle").writeBytes(it.readBytes()) }
        packaged.resolve("assets/retained.txt").writeText("retained asset\n"); packaged.resolve("obsolete.txt").writeText("delete me\n")
        fun open() = NeutronOTAStore(context, packaged, vector.getString("publicKey"), "test", "0.76.0", "1.0.0")
        val manifest = vector.getJSONObject("manifest").toString(); val canonical = vector.getString("canonical")
        var store = open()
        try {
            assertTrue(store.verify(canonical, vector.getJSONObject("manifest").getString("signature"), vector.getString("publicKeyPEM")))
            assertFalse(store.verify(canonical + " ", vector.getJSONObject("manifest").getString("signature"), vector.getString("publicKey")))
            store.selectBundleForBoot(); store.healthy(); store.begin(manifest, canonical)
            store.chunk("signed-fixture-1", "index.jsbundle", Base64.decode(vector.getString("chunkBase64"), Base64.DEFAULT))
            store.delete("signed-fixture-1", "obsolete.txt")
            assertEquals(vector.getJSONObject("manifest").getString("bundleHash"), store.stagedHash("signed-fixture-1"))
            store.commit(manifest, canonical); store.close()
            for (expected in 0..2) { store = open(); assertTrue(store.selectBundleForBoot().contains("signed-fixture-1")); assertEquals(expected, JSONObject(store.stateJSON()).getInt("consecutiveCrashes")); store.close() }
            store = open(); assertEquals(packaged.resolve("index.jsbundle").path, store.selectBundleForBoot()); assertEquals(1, JSONObject(store.stateJSON()).getInt("buildNumber"))
        } finally { store.close(); packaged.deleteRecursively() }
    }
}
