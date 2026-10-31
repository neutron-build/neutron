package org.neutron.ota
import com.facebook.react.BaseReactPackage
import com.facebook.react.bridge.NativeModule
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.module.model.ReactModuleInfo
import com.facebook.react.module.model.ReactModuleInfoProvider
class NeutronOTAPackage : BaseReactPackage() {
    override fun getModule(name: String, context: ReactApplicationContext): NativeModule? = if (name == "NeutronOTA") NeutronOTAModule(context) else null
    override fun getReactModuleInfoProvider() = ReactModuleInfoProvider {
        mapOf("NeutronOTA" to ReactModuleInfo("NeutronOTA", "NeutronOTA", false, false, false, true))
    }
}
