import { generateStaticParamsFor, importPage } from "nextra/pages";
import { useMDXComponents as getMDXComponents } from "../../../mdx-components";
import { locales } from "../../../asset-prefix.mjs";

export const generateStaticParams = async () => {
  const originalGenerateParams = generateStaticParamsFor("mdxPath");
  const params = await originalGenerateParams();

  // 为每个语言生成参数
  const allParams = [];

  for (const lang of locales) {
    // 过滤掉图片文件路径
    const filteredParams = params.filter((param) => {
      const path = Array.isArray(param.mdxPath)
        ? param.mdxPath.join("/")
        : param.mdxPath || "";
      return !path.match(/\.(png|jpg|jpeg|gif|svg|ico|webp)$/i);
    });

    // 为每个过滤后的参数添加语言参数
    for (const param of filteredParams) {
      allParams.push({
        ...param,
        lang: lang,
      });
    }

    // 确保为每个语言都添加根路径参数
    allParams.push({
      mdxPath: [],
      lang: lang,
    });
  }

  return allParams;
};

// 移除TS类型，仅用JS语法
export async function generateMetadata(props) {
  const params = await props.params;
  const { metadata } = await importPage(params.mdxPath, params.lang);
  return metadata;
}

// 不再声明TS类型
const Wrapper = getMDXComponents().wrapper;

const Page = async (props) => {
  const params = await props.params;
  const result = await importPage(params.mdxPath, params.lang);
  const { default: MDXContent, toc, metadata, sourceCode } = result;

  return (
    <Wrapper toc={toc} metadata={metadata} sourceCode={sourceCode}>
      <MDXContent {...props} params={params} />
    </Wrapper>
  );
};

export default Page;
