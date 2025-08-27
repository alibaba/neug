/* eslint-env node */

import type { Metadata } from "next";
import { Layout, Link, Navbar } from "nextra-theme-docs";
import { getPageMap } from "nextra/page-map";
import { LanguageDropdown } from "../../src/components/language-dropdown";
import type { FC, ReactNode } from "react";
import { locales } from "../../asset-prefix.mjs";
const i18n_maps = [
  { locale: "en", name: "English" },
  { locale: "zh", name: "中文" },
  { locale: "de", name: "Deutsch" },
  { locale: "fr", name: "Français" },
  { locale: "ru", name: "Русский" },
  { locale: "ja", name: "日本語" },
];

const i18n = i18n_maps.filter((lang) => {
  return locales.includes(lang.locale);
});

type LayoutProps = Readonly<{
  children: ReactNode;
  params: Promise<{
    lang: string;
  }>;
}>;

const LanguageLayout: FC<LayoutProps> = async ({ children, params }) => {
  const { lang } = await params;
  console.log("Language Layout - lang:", lang);

  let sourcePageMap = await getPageMap(`/${lang}`);

  const navbar = (
    <Navbar
      logo={
        <>
          <span
            className='ms-2 select-none font-extrabold flex items-center'
            title={`NeuG: HTAP Graph Database`}
          >
            <img
              src='https://img.alicdn.com/imgextra/i3/O1CN01DaSVLB1lD7ZIbDOi2_!!6000000004784-2-tps-256-257.png'
              alt='GraphScope Neug'
              width={32}
              height={32}
              className='inline-block align-middle mr-2 '
              style={{ verticalAlign: "middle" }}
            />
            <span className='text-lg font-bold align-middle text-gray-900 dark:text-white'>
              NeuG
            </span>
          </span>
        </>
      }
      projectLink='https://github.com/GraphScope/neug'
    >
      <LanguageDropdown currentLang={lang} />
    </Navbar>
  );

  return (
    <Layout
      // banner={banner}
      navbar={navbar}
      footer={null}
      docsRepositoryBase='https://github.com/GraphScope/neug/blob/main/doc'
      i18n={i18n}
      sidebar={{
        defaultMenuCollapseLevel: 1,
        autoCollapse: true,
      }}
      pageMap={sourcePageMap}
      nextThemes={{ defaultTheme: "light" }}
    >
      {children}
    </Layout>
  );
};

export const generateStaticParams = async () => {
  // 支持的语言列表

  return locales.map((lang) => ({
    lang: lang,
  }));
};

export default LanguageLayout;
