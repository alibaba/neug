const GITHUB_PAGE_PREFIX = "neug";
export const locales = ["en", "zh"];

export const getAssetPrefix = () => {
  const isProduction = process.env.NODE_ENV === "production";
  const assetPrefix = isProduction ? `/${GITHUB_PAGE_PREFIX}` : "";
  return assetPrefix;
};
