import XCTest
@testable import NeutronOTA

final class NeutronOTAStoreTests: XCTestCase {
    private func fixture() throws -> [String: Any] {
        let url = try XCTUnwrap(Bundle(for: Self.self).url(forResource: "vector", withExtension: "json"))
        return try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
    }
    func testAuthenticatedStageDeletionMonotonicPublicationAndNativeRestarts() throws {
        let v = try fixture(), manifest = v["manifest"] as! [String: Any], canonical = v["canonical"] as! String
        let base = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).resolvingSymlinksInPath()
        let packaged = base.appendingPathComponent("packaged")
        let fixtureDirectory = try XCTUnwrap(Bundle(for: Self.self).url(forResource: "packaged", withExtension: nil))
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        try FileManager.default.copyItem(at: fixtureDirectory, to: packaged)
        defer { try? FileManager.default.removeItem(at: base) }
        func open() throws -> NeutronOTAStore { try NeutronOTAStore(root: base.appendingPathComponent("store"), packagedBundleDirectory: packaged, publicKey: v["publicKey"] as! String, channel: "test", runtimeVersion: "0.76.0", appVersion: "1.0.0") }
        var store: NeutronOTAStore? = try open()
        XCTAssertTrue(try store!.verify(canonical, signature: manifest["signature"] as! String, publicKey: v["publicKeyPEM"] as! String))
        XCTAssertFalse(try store!.verify(canonical + " ", signature: manifest["signature"] as! String, publicKey: v["publicKey"] as! String))
        _ = try store!.selectBundleForBoot(); _ = try store!.healthy()
        let json = String(data: try JSONSerialization.data(withJSONObject: manifest), encoding: .utf8)!
        try store!.begin(json, canonical)
        XCTAssertThrowsError(try store!.chunk("signed-fixture-1", "../escape", Data()))
        XCTAssertThrowsError(try store!.chunk("signed-fixture-1", "index.jsbundle", Data("tampered".utf8)))
        try store!.chunk("signed-fixture-1", "index.jsbundle", Data(base64Encoded: v["chunkBase64"] as! String)!)
        try store!.delete("signed-fixture-1", "obsolete.txt")
        XCTAssertEqual(try store!.stagedHash("signed-fixture-1"), manifest["bundleHash"] as? String)
        _ = try store!.commit(json, canonical)
        XCTAssertThrowsError(try store!.begin(json, canonical))
        store = nil
        for expected in 0...2 {
            store = try open(); XCTAssertTrue(try store!.selectBundleForBoot().path.contains("signed-fixture-1"))
            let state = try JSONSerialization.jsonObject(with: Data(store!.stateJSON().utf8)) as! [String: Any]
            XCTAssertEqual(state["consecutiveCrashes"] as? Int, expected)
            store = nil // Native object restart fixture; process-kill tests below are separate.
        }
        store = try open(); XCTAssertEqual(try store!.selectBundleForBoot(), packaged.appendingPathComponent("index.jsbundle"))
        let rolled = try JSONSerialization.jsonObject(with: Data(store!.stateJSON().utf8)) as! [String: Any]
        XCTAssertEqual(rolled["buildNumber"] as? Int, 1); XCTAssertEqual(rolled["consecutiveCrashes"] as? Int, 0)
        store = nil
    }
}
