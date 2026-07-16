import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Native (iOS & Android) - Neutron",
    description: "Preact components rendered to native UIKit and Android views via React Native Fabric. Share UI code with web, develop with Expo Go, target iOS and Android from one codebase.",
  };
}

export default function NativePage() {
  return (
    <ProductPage
      title="Neutron Native"
      description="Preact components rendered to real UIKit and Android views via React Native Fabric. Same components run on web, iOS, and Android. Develop through Expo Go, targeting both stores from one build."
      category="platform"
      status="in-progress"
      accent="var(--accent-nucleus)"
      heroAccentRgb="0, 200, 83"
      heroTagline="Preact on the web. UIKit on the phone. Same code."
      stats={[
        { value: '3 KB', label: 'UI Runtime' },
        { value: 'Fabric', label: 'Native Renderer' },
        { value: 'Expo Go', label: 'Dev Workflow' },
        { value: '2', label: 'App Stores' },
      ]}
    >
      <section>
        <h2>No WebView. No bridge tax.</h2>
        <p>Neutron Native renders Preact components through React Native's Fabric renderer &mdash; the same path React Native uses, but with a 3 KB runtime instead of React's 42 KB. You get real UIKit on iOS and real Android views on Android, not a WebView pretending to be an app. Components are the same ones your web build uses, so sharing UI between platforms is just an import.</p>
      </section>

      <CodeBlock filename="src/routes/profile/[id].tsx" annotation="Same route. Web: DOM. iOS: UIKit. Android: Views.">
        <pre><code>{`import { useLoaderData } from "@neutron-build/core";
import { View, Text, Image } from "@neutron-build/native";

export async function loader({ params, ctx }) {
  const user = await ctx.db.sql()
    .query("SELECT id, name, avatar_url FROM users WHERE id = $1", [params.id])
    .one();
  return { user };
}

export default function Profile() {
  const { user } = useLoaderData<typeof loader>();
  return (
    <View class="profile">
      <Image src={user.avatar_url} class="profile__avatar" />
      <Text class="profile__name">{user.name}</Text>
    </View>
  );
}`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="0, 200, 83">
        <div class="feature-card">
          <div class="feature-card__title">Fabric renderer</div>
          <div class="feature-card__desc">React Native's new architecture renders Preact components to UIKit and Android views. No WebView, no bridge &mdash; synchronous native calls.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">3 KB UI runtime</div>
          <div class="feature-card__desc">Preact instead of React. 14× smaller runtime means less cold-start time, smaller app binaries, and more room for your code.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Expo Go compatible</div>
          <div class="feature-card__desc">Use the Expo Go app for dev and preview &mdash; no custom native client to maintain. Scan a QR code, see your changes in under 500ms.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Unified file routing</div>
          <div class="feature-card__desc">One <code>src/routes/</code> tree serves web, iOS, and Android. Platform-aware navigation, deep linking, and transitions handled by the framework.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Device modules</div>
          <div class="feature-card__desc">Camera, contacts, location, notifications, biometrics, haptics, share sheet. Typed APIs over React Native Turbo Modules.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Animation &amp; gestures</div>
          <div class="feature-card__desc">Reanimated-backed worklet animations run on the UI thread. Gesture handler for pan, pinch, long-press, double-tap. Typed accessibility.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['Feature', 'React Native', 'Neutron Native']}
        rows={[
          ['UI framework', 'React (~42 KB runtime)', 'Preact (~3 KB runtime)'],
          ['Web code sharing', 'Separate React Native Web lib', 'Same components, no adapter'],
          ['Renderer', 'Fabric', 'Fabric'],
          ['Build tool', 'Metro', 'Vite'],
          ['Dev client', 'Expo Go or custom', 'Expo Go'],
          ['Data layer', 'BYO (Redux, Zustand, &hellip;)', 'Loaders &amp; actions (same as web)'],
        ]}
        highlightColumn={2}
        accentRgb="0, 200, 83"
      />

      <section>
        <h3>What it's for</h3>
        <p>Cross-platform apps sharing the majority of UI code across web and mobile. Apps that ship as a marketing site on day one and an iOS/Android binary on day thirty, from the same codebase. Internal tools that have to work on a desktop browser and a field tech's Android tablet without maintaining two apps.</p>

        <h3>Why Fabric, not WebView?</h3>
        <p>Because WebView apps are obvious to users &mdash; scroll momentum is wrong, keyboards feel weird, native gestures don't work. Fabric renders to the actual OS controls, so your app feels like the platform. The tradeoff is a larger runtime than pure WebView, which Preact solves by being 3 KB instead of 42.</p>

        <h3>Part of a bigger system</h3>
        <p>Web + mobile + desktop from one codebase, all reading the same Nucleus database. Add a Rust service behind them for heavy lifting, Python for ML-backed features, Go for microservices &mdash; every platform front-end speaks the same protocol.</p>
      </section>
    </ProductPage>
  );
}
