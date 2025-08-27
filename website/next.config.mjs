import nextra from "nextra";
import { getAssetPrefix } from "./asset-prefix.mjs";

const withNextra = nextra({
  latex: true,
  search: {
    codeblocks: false,
  },
  contentDirBasePath: "/",
  unstable_shouldAddLocaleToLinks: true,
});

const assetPrefix = getAssetPrefix();
const locales = ["en", "zh", "de", "fr", "ru", "ja"];

export default withNextra({
  reactStrictMode: true,
  basePath: assetPrefix,
  assetPrefix: assetPrefix,
  i18n: {
    locales,
    defaultLocale: "en",
  },
  eslint: {
    ignoreDuringBuilds: true,
  },
  output: "export",
  trailingSlash: true,
  images: {
    unoptimized: true, // mandatory, otherwise won't export
  },
  redirects: async () => {
    return locales.map((lang) => ({
      source: `/${lang}`,
      destination: `/${lang}/overview/introduction`,
      statusCode: 302,
    }));
  },
});
