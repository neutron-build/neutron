import { defineCms } from "@neutron-build/cms";

export default defineCms({
  icon: "/favicon.svg",
  postUrl: (slug) => `/posts/${slug}`,
});
