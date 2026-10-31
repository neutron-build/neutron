package org.neutron.ota
import android.app.Activity
import android.os.Bundle
import android.widget.TextView
import android.util.Base64
import org.json.JSONObject
import java.io.File
/** Acceptance-app-only Activity. Never register in a production manifest. */
class NeutronOTAProcessFixture : Activity() {
    override fun onCreate(saved: Bundle?) {
        super.onCreate(saved)
        val vector = JSONObject(assets.open("vector.json").bufferedReader().use { it.readText() })
        val packaged = File(filesDir, "ota-original")
        if (!packaged.exists()) {
            packaged.mkdirs(); packaged.resolve("assets").mkdir()
            listOf("index.jsbundle", "obsolete.txt", "assets/retained.txt").forEach { file -> assets.open("packaged/$file").use { packaged.resolve(file).writeBytes(it.readBytes()) } }
        }
        val store = NeutronOTAStore(this, packaged, vector.getString("publicKey"), "test", "0.76.0", "1.0.0")
        NeutronOTAStore.shared = store
        store.selectBundleForBoot() // Native pre-JS path selection in a fresh process.
        when (intent.getStringExtra("ota_phase")) {
            "stage" -> {
                store.healthy(); val json = vector.getJSONObject("manifest").toString(); val canonical = vector.getString("canonical")
                store.begin(json, canonical); store.chunk("signed-fixture-1", "index.jsbundle", Base64.decode(vector.getString("chunkBase64"), Base64.DEFAULT)); store.delete("signed-fixture-1", "obsolete.txt"); store.commit(json, canonical)
            }
            "healthy" -> store.healthy()
            "interrupted-stage" -> { store.healthy(); store.begin(vector.getJSONObject("manifest").toString(), vector.getString("canonical")) }
        }
        File(filesDir, "ota-observed.json").writeText(store.stateJSON())
        setContentView(TextView(this).apply { text = store.stateJSON(); contentDescription = "ota-state" })
    }
}
