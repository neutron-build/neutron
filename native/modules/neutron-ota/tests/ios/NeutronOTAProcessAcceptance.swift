import XCTest
/** Requires the separate acceptance app fixture below, not a mocked RN process. */
final class NeutronOTAProcessAcceptance: XCTestCase {
    func testKilledProcessesRollbackOnFourthBootAndHealthyConfirmation() throws {
        let app = XCUIApplication()
        func launch(_ phase: String) throws -> [String: Any] {
            app.terminate(); app.launchArguments = ["--ota-phase", phase]; app.launch()
            let label = app.staticTexts["ota-state"]; XCTAssertTrue(label.waitForExistence(timeout: 10))
            return try JSONSerialization.jsonObject(with: Data(label.label.utf8)) as! [String: Any]
        }
        XCTAssertEqual(try launch("stage")["pendingUpdateId"] as? String, "signed-fixture-1")
        for expected in 0...2 { let state = try launch("boot"); XCTAssertEqual(state["currentUpdateId"] as? String, "signed-fixture-1"); XCTAssertEqual(state["consecutiveCrashes"] as? Int, expected) }
        let rolled = try launch("boot"); XCTAssertTrue(rolled["currentUpdateId"] is NSNull); XCTAssertEqual(rolled["buildNumber"] as? Int, 1)
        let healthy = try launch("healthy"); XCTAssertEqual(healthy["launchPending"] as? Bool, false)
    }
}
