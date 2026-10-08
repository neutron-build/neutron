import UIKit
import NeutronOTA
/** Add only to a fresh, separate installed acceptance app target. */
@main final class OTAProcessFixtureApp: UIResponder, UIApplicationDelegate {
    var window: UIWindow?
    func application(_ app: UIApplication, didFinishLaunchingWithOptions options: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        do {
            let args = ProcessInfo.processInfo.arguments
            let phase = args.firstIndex(of: "--ota-phase").map { args[$0 + 1] } ?? "boot"
            let vector = try JSONSerialization.jsonObject(with: Data(contentsOf: Bundle.main.url(forResource: "vector", withExtension: "json")!)) as! [String: Any]
            let packaged = Bundle.main.url(forResource: "packaged", withExtension: nil)!
            let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0].appendingPathComponent("neutron-ota")
            let store = try NeutronOTAStore(root: root, packagedBundleDirectory: packaged, publicKey: vector["publicKey"] as! String, channel: "test", runtimeVersion: "0.76.0", appVersion: "1.0.0")
            NeutronOTAStore.shared = store; _ = try store.selectBundleForBoot()
            if phase == "stage" {
                _ = try store.healthy(); let canonical = vector["canonical"] as! String
                let json = String(data: try JSONSerialization.data(withJSONObject: vector["manifest"]!), encoding: .utf8)!
                try store.begin(json, canonical); try store.chunk("signed-fixture-1", "index.jsbundle", Data(base64Encoded: vector["chunkBase64"] as! String)!)
                try store.delete("signed-fixture-1", "obsolete.txt"); _ = try store.commit(json, canonical)
            } else if phase == "healthy" { _ = try store.healthy() }
            let controller = UIViewController(), label = UILabel(frame: CGRect(x: 10, y: 100, width: 380, height: 500))
            label.text = try store.stateJSON(); label.accessibilityIdentifier = "ota-state"; label.numberOfLines = 0; controller.view.addSubview(label)
            window = UIWindow(frame: UIScreen.main.bounds); window!.rootViewController = controller; window!.makeKeyAndVisible(); return true
        } catch { fatalError("Native fixture failed: \(error)") }
    }
}
