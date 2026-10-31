import Foundation
import CryptoKit
import Darwin

/// One process-wide store, configured by the host BEFORE creating any RN bridge.
/// The app must not load its bundled URL after calling selectBundleForBoot().
@objc(NeutronOTAStore) public final class NeutronOTAStore: NSObject {
    @objc public static var shared: NeutronOTAStore?
    public let runtimeVersion: String
    public let appVersion: String
    private let channel: String, key: Data, root: URL, packaged: URL
    private let lock = NSRecursiveLock()
    public private(set) var launchEpoch = UUID().uuidString
    private var bootSelected = false
    private var processLock: Int32 = -1
    private var admitted: [String: [String: Any]] = [:]
    private let maxBytes = 64 * 1024 * 1024
    private var stateURL: URL { root.appendingPathComponent("state.json") }
    private func error(_ message: String) -> NSError { NSError(domain: "NeutronOTA", code: 1, userInfo: [NSLocalizedDescriptionKey: message]) }
    @objc public init(root: URL, packagedBundleDirectory: URL, publicKey: String, channel: String, runtimeVersion: String, appVersion: String) throws {
        self.root = root.resolvingSymlinksInPath(); self.packaged = packagedBundleDirectory.resolvingSymlinksInPath(); self.channel = channel
        self.runtimeVersion = runtimeVersion; self.appVersion = appVersion
        self.key = try Self.decodeKey(publicKey)
        super.init()
        try directory(self.root)
        let lockURL = self.root.appendingPathComponent(".owner.lock")
        processLock = open(lockURL.path, O_RDWR | O_CREAT | O_NOFOLLOW, 0o600)
        guard processLock >= 0, flock(processLock, LOCK_EX | LOCK_NB) == 0 else { if processLock >= 0 { close(processLock); processLock = -1 }; throw error("OTA store already owned by another process") }
        if !FileManager.default.fileExists(atPath: stateURL.path) { try save(empty()) }
        _ = try read()
    }
    deinit { if processLock >= 0 { flock(processLock, LOCK_UN); close(processLock) } }
    private static func decodeKey(_ text: String) throws -> Data {
        let body = text.replacingOccurrences(of: "-----BEGIN PUBLIC KEY-----", with: "").replacingOccurrences(of: "-----END PUBLIC KEY-----", with: "").filter { !$0.isWhitespace }
        guard let data = Data(base64Encoded: body) else { throw NSError(domain: "NeutronOTA", code: 2) }
        if data.count == 32 { return data }
        let prefix = Data([0x30,0x2a,0x30,0x05,0x06,0x03,0x2b,0x65,0x70,0x03,0x21,0x00])
        guard data.count == 44, data.prefix(12) == prefix else { throw NSError(domain: "NeutronOTA", code: 2) }
        return data.suffix(32)
    }
    private func empty() -> [String: Any] { ["currentUpdateId": NSNull(), "lastGoodUpdateId": NSNull(), "pendingUpdateId": NSNull(), "buildNumber": 0, "consecutiveCrashes": 0, "launchPending": false] }
    private func id(_ text: String) throws -> String {
        guard text.range(of: "^[A-Za-z0-9_-]{1,128}$", options: .regularExpression) != nil else { throw error("Invalid update ID") }; return text
    }
    private func noLinks(_ url: URL) throws {
        var prefix = URL(fileURLWithPath: "/")
        for component in url.standardizedFileURL.pathComponents.dropFirst() {
            prefix.appendPathComponent(component)
            var stat = Darwin.stat()
            if lstat(prefix.path, &stat) == 0, stat.st_mode & S_IFMT == S_IFLNK { throw error("Linked storage path") }
        }
    }
    private func directory(_ url: URL) throws {
        try noLinks(url)
        if !FileManager.default.fileExists(atPath: url.path) {
            let parent = url.deletingLastPathComponent()
            if !FileManager.default.fileExists(atPath: parent.path) { try directory(parent) }
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            try sync(parent)
        }
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
        guard attributes[.type] as? FileAttributeType == .typeDirectory, (attributes[.ownerAccountID] as? NSNumber)?.uint32Value == getuid() else { throw error("Storage not owned by app") }
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
    }
    private func sync(_ url: URL) throws {
        let descriptor = open(url.path, O_RDONLY | O_NOFOLLOW)
        guard descriptor >= 0 else { throw error("Cannot open directory for sync") }
        defer { close(descriptor) }
        guard fsync(descriptor) == 0 else { throw error("Directory sync failed") }
    }
    private func publish(_ data: Data, _ url: URL) throws {
        try directory(url.deletingLastPathComponent()); try noLinks(url)
        let temp = url.deletingLastPathComponent().appendingPathComponent(".\(UUID().uuidString).tmp")
        let descriptor = open(temp.path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600)
        guard descriptor >= 0 else { throw error("Cannot create private stage") }
        defer { close(descriptor); try? FileManager.default.removeItem(at: temp) }
        try data.withUnsafeBytes { raw in
            var offset = 0
            while offset < raw.count {
                let count = Darwin.write(descriptor, raw.baseAddress!.advanced(by: offset), raw.count - offset)
                if count <= 0 { throw error("Stage write failed") }; offset += count
            }
        }
        guard fsync(descriptor) == 0, rename(temp.path, url.path) == 0 else { throw error("Atomic publication failed") }
        try sync(url.deletingLastPathComponent())
    }
    private func read() throws -> [String: Any] {
        try noLinks(stateURL)
        let data = try Data(contentsOf: stateURL)
        guard data.count <= 16384, let state = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let build = state["buildNumber"] as? NSNumber, build.doubleValue >= 0, build.doubleValue <= 9007199254740991, build.doubleValue.rounded() == build.doubleValue,
              let crashes = state["consecutiveCrashes"] as? NSNumber, crashes.intValue >= 0, crashes.intValue <= 1000000, crashes.doubleValue.rounded() == crashes.doubleValue, state["launchPending"] is Bool else { throw error("Corrupt boot state") }
        for field in ["currentUpdateId", "lastGoodUpdateId", "pendingUpdateId"] {
            if let value = state[field] as? String { _ = try id(value) } else if !(state[field] is NSNull) { throw error("Corrupt boot identity") }
        }
        return state
    }
    private func save(_ state: [String: Any]) throws { try publish(JSONSerialization.data(withJSONObject: state, options: [.sortedKeys]), stateURL) }
    private func bundle(_ name: String) throws -> URL { root.appendingPathComponent("bundles").appendingPathComponent(try id(name)) }
    private func stage(_ name: String) throws -> URL { root.appendingPathComponent("stages").appendingPathComponent(try id(name)) }
    private func path(_ directory: URL, _ relative: String) throws -> URL {
        let parts = relative.split(separator: "/", omittingEmptySubsequences: false)
        guard relative.utf8.count <= 1024, !relative.contains("\\"), !relative.contains("\0"), parts.allSatisfy({ !$0.isEmpty && $0 != "." && $0 != ".." }) else { throw error("Invalid bundle path") }
        let url = directory.appendingPathComponent(relative); try noLinks(url); return url
    }
    private func files(_ directory: URL) throws -> [URL] {
        try noLinks(directory)
        var result: [URL] = []
        for url in try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: [.isDirectoryKey, .isRegularFileKey, .isSymbolicLinkKey]) {
            let values = try url.resourceValues(forKeys: [.isDirectoryKey, .isRegularFileKey, .isSymbolicLinkKey])
            guard values.isSymbolicLink != true else { throw error("Bundle contains link") }
            if values.isDirectory == true { result += try files(url) }
            else if values.isRegularFile == true { result.append(url) } else { throw error("Bundle contains special file") }
            guard result.count <= 4096 else { throw error("Bundle file count exceeded") }
        }
        return result
    }
    /// Digest = SHA256(sorted UTF-8 path + NUL + lowercase file SHA256 + LF).
    /// Each file digest streams; unchanged files and deletes affect the inventory.
    public func digest(_ directory: URL) throws -> String {
        var assembled = SHA256(), total = 0
        for url in try files(directory).sorted(by: { $0.path.utf8.lexicographicallyPrecedes($1.path.utf8) }) {
            let handle = try FileHandle(forReadingFrom: url); defer { try? handle.close() }
            var file = SHA256()
            while let bytes = try handle.read(upToCount: 65536), !bytes.isEmpty { total += bytes.count; guard total <= maxBytes else { throw error("Assembled bundle too large") }; file.update(data: bytes) }
            let relative = String(url.path.dropFirst(directory.path.count + 1))
            let hash = file.finalize().map { String(format: "%02x", $0) }.joined()
            assembled.update(data: Data((relative + "\0" + hash + "\n").utf8))
        }
        return assembled.finalize().map { String(format: "%02x", $0) }.joined()
    }
    public func verify(_ canonical: String, signature: String, publicKey: String) throws -> Bool {
        guard try Self.decodeKey(publicKey) == key, let bytes = Data(base64Encoded: signature), bytes.count == 64 else { return false }
        return try Curve25519.Signing.PublicKey(rawRepresentation: key).isValidSignature(bytes, for: Data(canonical.utf8))
    }
    private func manifest(_ json: String, _ canonical: String, admission: Bool = true) throws -> [String: Any] {
        guard json.utf8.count <= 1048576, canonical.utf8.count <= 1048576,
              let m = try JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any],
              let signed = try JSONSerialization.jsonObject(with: Data(canonical.utf8)) as? [String: Any],
              let signature = m["signature"] as? String,
              try verify(canonical, signature: signature, publicKey: key.base64EncodedString()),
              m["channel"] as? String == channel, m["runtimeVersion"] as? String == runtimeVersion,
              let build = m["buildNumber"] as? NSNumber, build.doubleValue.rounded() == build.doubleValue, build.doubleValue <= 9007199254740991,
              (!admission || build.doubleValue > (try read()["buildNumber"] as! NSNumber).doubleValue),
              let chunks = m["chunks"] as? [[String: Any]], chunks.count <= 4096,
              let size = m["downloadSize"] as? NSNumber, size.intValue >= 0, size.intValue <= maxBytes else { throw error("Unauthenticated or incompatible manifest") }
        let fields = ["id", "version", "buildNumber", "runtimeVersion", "channel", "bundleHash", "downloadSize", "createdAt", "minAppVersion", "chunks"]
        var normalized = [String: Any]()
        for field in fields { normalized[field] = m[field] ?? NSNull() }
        guard NSDictionary(dictionary: normalized).isEqual(to: signed) else { throw error("Manifest differs from signed fields") }
        _ = try id(m["id"] as? String ?? "")
        var paths = Set<String>(), total = 0
        for chunk in chunks {
            let relative = chunk["path"] as? String ?? ""; _ = try path(root, relative)
            guard paths.insert(relative).inserted, let bytes = chunk["size"] as? Int, bytes >= 0, bytes <= maxBytes,
                  let operation = chunk["operation"] as? String, ["add", "modify", "delete"].contains(operation) else { throw error("Invalid chunk") }
            if operation == "delete" { guard bytes == 0 else { throw error("Delete payload") } }
            else { guard let hash = chunk["hash"] as? String, hash.range(of: "^[a-f0-9]{64}$", options: .regularExpression) != nil, let url = URL(string: chunk["url"] as? String ?? ""), url.scheme == "https" else { throw error("Invalid chunk digest/URL") }; total += bytes }
        }
        guard total == size.intValue, (m["bundleHash"] as? String)?.range(of: "^[a-f0-9]{64}$", options: .regularExpression) != nil else { throw error("Invalid bundle size/digest") }
        if let minimum = m["minAppVersion"] as? String {
            func version(_ text: String) throws -> [Int] { let parts = text.split(separator: ".").compactMap { Int($0) }; guard parts.count == 3, parts.allSatisfy({ $0 >= 0 }) else { throw error("Invalid app version") }; return parts }
            let current = try version(appVersion), required = try version(minimum)
            guard !current.lexicographicallyPrecedes(required) else { throw error("Newer native app required") }
        }
        return m
    }
    public func stateJSON() throws -> String { lock.lock(); defer { lock.unlock() }; return String(data: try JSONSerialization.data(withJSONObject: read()), encoding: .utf8)! }
    public func healthy() throws -> String { lock.lock(); defer { lock.unlock() }; var s = try read(); guard bootSelected else { throw error("Boot not selected") }; s["lastGoodUpdateId"] = s["currentUpdateId"]; s["launchPending"] = false; s["consecutiveCrashes"] = 0; try save(s); return try stateJSON() }
    public func crash() throws -> String { lock.lock(); defer { lock.unlock() }; var s = try read(); s["consecutiveCrashes"] = (s["consecutiveCrashes"] as! Int) + 1; s["launchPending"] = false; try save(s); return try stateJSON() }
    public func rollback() throws -> String { lock.lock(); defer { lock.unlock() }; var s = try read(); s["currentUpdateId"] = s["lastGoodUpdateId"]; s["pendingUpdateId"] = NSNull(); s["consecutiveCrashes"] = 0; s["launchPending"] = false; try save(s); return try stateJSON() }
    @objc public func selectBundleForBoot() throws -> URL {
        lock.lock(); defer { lock.unlock() }
        if bootSelected { return try selectedURL(read()) }
        var s = try read()
        if s["launchPending"] as? Bool == true { s["consecutiveCrashes"] = (s["consecutiveCrashes"] as! Int) + 1 }
        if (s["consecutiveCrashes"] as! Int) >= 3 { s["currentUpdateId"] = s["lastGoodUpdateId"]; s["pendingUpdateId"] = NSNull(); s["consecutiveCrashes"] = 0 }
        else if let pending = s["pendingUpdateId"] as? String { s["consecutiveCrashes"] = 0; s["currentUpdateId"] = pending; s["pendingUpdateId"] = NSNull() }
        let url = try selectedURL(s)
        s["launchPending"] = true; try save(s); launchEpoch = UUID().uuidString; bootSelected = true; return url
    }
    private func selectedURL(_ state: [String: Any]) throws -> URL {
        let directory = try (state["currentUpdateId"] as? String).map { try bundle($0) } ?? packaged
        if let name = state["currentUpdateId"] as? String {
            let receiptURL = root.appendingPathComponent("receipts").appendingPathComponent(try id(name) + ".json")
            try noLinks(receiptURL)
            let bytes = try Data(contentsOf: receiptURL); guard bytes.count <= 2097152 else { throw error("Oversized bundle receipt") }
            guard let receipt = try JSONSerialization.jsonObject(with: bytes) as? [String: String], let json = receipt["manifest"], let canonical = receipt["canonical"] else { throw error("Corrupt bundle receipt") }
            let m = try manifest(json, canonical, admission: false)
            guard m["id"] as? String == name, try digest(directory) == m["bundleHash"] as? String else { throw error("Boot bundle digest differs from authenticated receipt") }
        }
        let url = try path(directory, "index.jsbundle")
        guard FileManager.default.fileExists(atPath: url.path) else { throw error("Selected JS bundle missing") }; return url
    }
    public func begin(_ json: String, _ canonical: String) throws {
        lock.lock(); defer { lock.unlock() }; guard admitted.isEmpty, try read()["pendingUpdateId"] is NSNull else { throw error("Another update owns staging or pending boot") }; let m = try manifest(json, canonical), name = m["id"] as! String, dest = try stage(name)
        guard !FileManager.default.fileExists(atPath: dest.path) else { throw error("Stage already exists") }
        try directory(dest)
        let s = try read(), source = try (s["currentUpdateId"] as? String).map { try bundle($0) } ?? packaged
        _ = try digest(source)
        for file in try files(source) { let target = try path(dest, String(file.path.dropFirst(source.path.count + 1))); try publish(Data(contentsOf: file), target) }
        admitted[name] = m
    }
    public func chunk(_ name: String, _ relative: String, _ bytes: Data) throws {
        lock.lock(); defer { lock.unlock() }
        guard let m = admitted[name], let c = (m["chunks"] as! [[String: Any]]).first(where: { $0["path"] as? String == relative }), c["operation"] as? String != "delete", c["size"] as? Int == bytes.count, bytes.count <= maxBytes,
              SHA256.hash(data: bytes).map({ String(format: "%02x", $0) }).joined() == c["hash"] as? String else { throw error("Unadmitted chunk") }
        try publish(bytes, path(stage(name), relative))
    }
    public func delete(_ name: String, _ relative: String) throws {
        lock.lock(); defer { lock.unlock() }; guard let m = admitted[name], (m["chunks"] as! [[String: Any]]).contains(where: { $0["path"] as? String == relative && $0["operation"] as? String == "delete" }) else { throw error("Unadmitted delete") }
        let url = try path(stage(name), relative)
        if FileManager.default.fileExists(atPath: url.path) { try FileManager.default.removeItem(at: url); try sync(url.deletingLastPathComponent()) }
    }
    public func stagedHash(_ name: String) throws -> String { lock.lock(); defer { lock.unlock() }; guard admitted[name] != nil else { throw error("Unknown stage") }; return try digest(stage(name)) }
    public func commit(_ json: String, _ canonical: String) throws -> String {
        lock.lock(); defer { lock.unlock() }; let m = try manifest(json, canonical), name = m["id"] as! String
        guard let original = admitted[name], NSDictionary(dictionary: original).isEqual(to: m), try digest(stage(name)) == m["bundleHash"] as? String else { throw error("Stage not admitted/assembled digest differs") }
        let dest = try bundle(name); try directory(dest.deletingLastPathComponent())
        guard !FileManager.default.fileExists(atPath: dest.path) else { throw error("Immutable bundle already exists") }
        _ = try path(stage(name), "index.jsbundle").checkResourceIsReachable()
        try FileManager.default.moveItem(at: stage(name), to: dest); try sync(dest.deletingLastPathComponent()); try sync(root.appendingPathComponent("stages"))
        try publish(JSONSerialization.data(withJSONObject: ["manifest": json, "canonical": canonical]), root.appendingPathComponent("receipts").appendingPathComponent(name + ".json"))
        var s = try read(); s["pendingUpdateId"] = name; s["buildNumber"] = m["buildNumber"]; try save(s); admitted.removeValue(forKey: name); return try stateJSON()
    }
    public func discard(_ name: String) throws { lock.lock(); defer { lock.unlock() }; let url = try stage(name); if FileManager.default.fileExists(atPath: url.path) { try FileManager.default.removeItem(at: url); try sync(url.deletingLastPathComponent()) }; admitted.removeValue(forKey: name) }
    public var hasBootSelection: Bool { lock.lock(); defer { lock.unlock() }; return bootSelected }
    public func prepareCurrentReload() throws { lock.lock(); defer { lock.unlock() }; bootSelected = false; _ = try selectBundleForBoot() }
    public func prepareReload(_ name: String) throws { lock.lock(); defer { lock.unlock() }; let s = try read(); guard s["pendingUpdateId"] as? String == name else { throw error("Reload ID not pending") }; bootSelected = false; _ = try selectBundleForBoot() }
}
