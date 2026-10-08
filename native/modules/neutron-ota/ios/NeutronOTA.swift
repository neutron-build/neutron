import Foundation
import CryptoKit
import React
@objc(NeutronOTA) final class NeutronOTA: NSObject {
    @objc static func requiresMainQueueSetup() -> Bool { false }
    private let launchEpoch = NeutronOTAStore.shared?.launchEpoch
    private let slots = DispatchSemaphore(value: 8)
    private let queue = DispatchQueue(label: "org.neutron.ota", qos: .utility)
    @objc func constantsToExport() -> [String: Any] {
        guard let store = NeutronOTAStore.shared else { return ["runtimeVersion": "", "appVersion": "", "nativeBootTracking": false] }
        return ["runtimeVersion": store.runtimeVersion, "appVersion": store.appVersion, "nativeBootTracking": store.hasBootSelection]
    }
    private func run(_ resolve: @escaping RCTPromiseResolveBlock, _ reject: @escaping RCTPromiseRejectBlock, _ work: @escaping (NeutronOTAStore) throws -> Any?) {
        guard slots.wait(timeout: .now()) == .success else { reject("OTA_BUSY", "Native OTA queue full", nil); return }
        queue.async {
            defer { self.slots.signal() }
            do { guard let store = NeutronOTAStore.shared, store.hasBootSelection, store.launchEpoch == self.launchEpoch else { throw NSError(domain: "NeutronOTA", code: 1, userInfo: [NSLocalizedDescriptionKey: "Configure native OTA and select its bundle before JS"] ) }; resolve(try work(store)) }
            catch { reject("OTA_ERROR", error.localizedDescription, error) }
        }
    }
    @objc func readBootState(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.stateJSON() } }
    @objc func markHealthy(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.healthy() } }
    @objc func recordCrash(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.crash() } }
    @objc func rollback(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { store in let state = try store.rollback(); try store.prepareCurrentReload(); DispatchQueue.main.async { RCTTriggerReloadCommandListeners("Neutron OTA rollback") }; return state } }
    @objc func verifyManifest(_ canonical: String, signature: String, publicKey: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.verify(canonical, signature: signature, publicKey: publicKey) } }
    @objc func sha256(_ bytes: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { _ in guard bytes.utf8.count <= 89478488, let data = Data(base64Encoded: bytes) else { throw NSError(domain: "NeutronOTA", code: 2) }; return SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined() } }
    @objc func beginStage(_ json: String, canonical: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.begin(json, canonical); return nil } }
    @objc func stageChunk(_ id: String, path: String, bytes: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { guard bytes.utf8.count <= 89478488, let data = Data(base64Encoded: bytes) else { throw NSError(domain: "NeutronOTA", code: 2) }; try $0.chunk(id, path, data); return nil } }
    @objc func deleteStagedPath(_ id: String, path: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.delete(id, path); return nil } }
    @objc func stagedBundleHash(_ id: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.stagedHash(id) } }
    @objc func publishPending(_ json: String, canonical: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.commit(json, canonical) } }
    @objc func discardStage(_ id: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) { run(resolve, reject) { try $0.discard(id); return nil } }
    @objc func reload(_ id: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        run(resolve, reject) { store in
            try store.prepareReload(id)
            DispatchQueue.main.async { RCTTriggerReloadCommandListeners("Neutron verified OTA") }
            return nil
        }
    }
}
