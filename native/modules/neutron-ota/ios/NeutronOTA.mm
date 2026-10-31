#import <React/RCTBridgeModule.h>
#ifdef RCT_NEW_ARCH_ENABLED
#import <NeutronOTASpec/NeutronOTASpec.h>
#endif
@interface RCT_EXTERN_MODULE(NeutronOTA, NSObject)
RCT_EXTERN_METHOD(readBootState:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(markHealthy:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(recordCrash:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(rollback:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(verifyManifest:(NSString *)canonical signature:(NSString *)signature publicKey:(NSString *)publicKey resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(sha256:(NSString *)bytes resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(beginStage:(NSString *)json canonical:(NSString *)canonical resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(stageChunk:(NSString *)updateId path:(NSString *)path bytes:(NSString *)bytes resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(deleteStagedPath:(NSString *)updateId path:(NSString *)path resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(stagedBundleHash:(NSString *)updateId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(publishPending:(NSString *)json canonical:(NSString *)canonical resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(discardStage:(NSString *)updateId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
RCT_EXTERN_METHOD(reload:(NSString *)updateId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject)
@end

#ifdef RCT_NEW_ARCH_ENABLED
// Codegen emits this interface/JSI wrapper from src/NativeNeutronOTA.ts.
// No generated files are checked in or synthesized by hand.
@interface NeutronOTA (TurboModule) <NativeNeutronOTASpec>
- (NSDictionary *)constantsToExport;
@end
@implementation NeutronOTA (TurboModule)
- (NSDictionary *)getConstants { return [self constantsToExport]; }
- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:
    (const facebook::react::ObjCTurboModule::InitParams &)params {
  return std::make_shared<facebook::react::NativeNeutronOTASpecJSI>(params);
}
@end
#endif
