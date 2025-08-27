"use client";

import React, { useState, useEffect } from "react";
import cn from "clsx";
import NextLink from "next/link";
import { useRouter } from "next/navigation";
import { GitHubIcon, DiscordIcon } from "nextra/icons";
import { Button } from "nextra/components";
import { FileText, Star, BookOpen } from "lucide-react";
import { ThemeToggle } from "./theme-toggle";

// 定义接口类型
interface NavbarProps {
  logo?: React.ReactNode;
  logoLink?: boolean | string;
  projectLink?: string;
  projectIcon?: React.ReactNode;
  chatLink?: string;
  chatIcon?: React.ReactNode;
  className?: string;
  align?: "left" | "right";
  children?: React.ReactNode;
}

// 默认图标
const defaultGitHubIcon = (
  <GitHubIcon height='24' aria-label='Project repository' />
);
const defaultChatIcon = <DiscordIcon width='24' />;

// 获取GitHub仓库stars数量
const useGitHubStars = (repoUrl?: string) => {
  const [stars, setStars] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!repoUrl) return;

    const getRepoInfo = (url: string) => {
      const match = url.match(/github\.com\/([^\/]+)\/([^\/]+)/);
      if (match) {
        return { owner: match[1], repo: match[2] };
      }
      return null;
    };

    const fetchStars = async () => {
      const repoInfo = getRepoInfo(repoUrl);
      if (!repoInfo) return;

      setLoading(true);
      try {
        const response = await fetch(
          `https://api.github.com/repos/${repoInfo.owner}/${repoInfo.repo}`
        );
        if (response.ok) {
          const data = await response.json();
          setStars(data.stargazers_count);
        }
      } catch (error) {
        console.error("Failed to fetch GitHub stars:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchStars();
  }, [repoUrl]);

  return { stars, loading };
};

// 获取用户语言的函数
const getUserLanguage = (): string => {
  if (typeof window === "undefined") return "en";

  // 首先从URL路径中获取语言
  const pathname = window.location.pathname;
  const pathSegments = pathname.split("/").filter(Boolean);

  if (pathSegments.length > 0) {
    const possibleLang = pathSegments[0];
    const supportedLanguages = ["en", "zh", "de", "fr", "ru", "ja"];
    if (supportedLanguages.includes(possibleLang)) {
      return possibleLang;
    }
  }

  // 如果URL中没有语言信息，使用浏览器语言
  const browserLang = navigator.language.toLowerCase();
  if (browserLang.startsWith("zh")) return "zh";
  if (browserLang.startsWith("de")) return "de";
  if (browserLang.startsWith("fr")) return "fr";
  if (browserLang.startsWith("ru")) return "ru";
  if (browserLang.startsWith("ja")) return "ja";

  return "en"; // 默认英文
};

// 自定义 Navbar 组件，完全复制 nextra-theme-docs 的样式和结构
export const CustomNavbar: React.FC<NavbarProps> = ({
  children,
  logoLink = true,
  logo,
  projectLink,
  projectIcon = defaultGitHubIcon,
  chatLink,
  chatIcon = defaultChatIcon,
  className,
  align = "right",
}) => {
  const { stars, loading } = useGitHubStars(projectLink);

  // 计算 logo 的对齐方式
  const logoAlignClass =
    align === "left" ? "max-md:me-auto" : "max-md:me-0 md:me-auto";

  // 主容器样式 - 完全复制 nextra-theme-docs 的样式
  const headerClass = cn(
    "nextra-navbar sticky top-0 z-30 w-full print:hidden",
    "max-md:[.nextra-banner:not([class$=hidden])~&]:top-[var(--nextra-banner-height)]"
  );

  // 背景模糊层 - 更高透明度融入背景
  const blurClass = cn(
    "nextra-navbar-blur",
    "absolute -z-10 size-full",
    "backdrop-blur-sm bg-white/20 dark:bg-slate-900/20",
    "border-b border-transparent"
  );

  // 导航栏容器样式
  const navClass = cn(
    "mx-auto flex max-w-[var(--nextra-content-width)] items-center gap-2 md:gap-4",
    "pl-[max(env(safe-area-inset-left),1.5rem)] pr-[max(env(safe-area-inset-right),1.5rem)]",
    "justify-between md:justify-end",
    className
  );

  // Logo 容器样式
  const logoClass = cn("flex items-center", logoAlignClass);

  // Logo 元素
  const logoElement = logoLink ? (
    <NextLink
      href={typeof logoLink === "string" ? logoLink : "/"}
      className={cn(
        logoClass,
        "transition-opacity focus-visible:nextra-focus hover:opacity-75"
      )}
      aria-label='Home page'
    >
      {logo}
    </NextLink>
  ) : (
    <div className={logoClass}>{logo}</div>
  );

  // 项目链接样式
  const linkClass = cn(
    "text-sm contrast-more:text-gray-700 contrast-more:dark:text-gray-100 whitespace-nowrap",
    "text-gray-600 hover:text-black dark:text-gray-400 dark:hover:text-gray-200",
    "ring-inset transition-colors"
  );

  // Document 链接样式 - 使用与 nextra navbar 中的链接一致的样式
  const documentLinkClass = cn(
    "text-sm contrast-more:text-gray-700 contrast-more:dark:text-gray-100 whitespace-nowrap",
    "text-gray-800 hover:text-black dark:text-gray-300 dark:hover:text-gray-100",
    "ring-inset transition-colors",
    "px-3 py-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800"
  );

  // 获取文档链接
  const getDocumentLink = () => {
    const userLang = getUserLanguage();
    return `/${userLang}`;
  };

  // 右侧导航区域的对齐方式
  const rightAlignClass =
    align === "left" ? "me-auto" : "max-md:ml-auto md:ml-0";

  return (
    <header className={headerClass}>
      {/* 背景模糊层 */}
      <div className={blurClass} />

      {/* 主导航栏 */}
      <nav
        style={{ height: "var(--nextra-navbar-height)" }}
        className={navClass}
      >
        {/* Logo 区域 */}
        {logoElement}

        {/* 移动端 Document 链接 - 始终显示 */}
        <NextLink
          href={getDocumentLink()}
          className={cn(
            documentLinkClass,
            "flex items-center md:hidden",
            "text-sm px-2 py-1 rounded-md"
          )}
          aria-label='Documentation'
        >
          <BookOpen height='20' />
        </NextLink>

        {/* 右侧导航区域 - 桌面端 */}
        <div
          className={cn(
            "flex gap-4 overflow-x-auto nextra-scrollbar py-1.5 max-md:hidden",
            rightAlignClass
          )}
        >
          {/* Document 链接 - 桌面端 */}
          <NextLink
            href={getDocumentLink()}
            className={cn(documentLinkClass, "flex items-center")}
            aria-label='Documentation'
          >
            <BookOpen height='24' className='mr-1.5' />
            Documentation
          </NextLink>

          {/* 项目链接 */}
          {projectLink && (
            <a
              href={projectLink}
              target='_blank'
              rel='noopener noreferrer'
              className={cn(linkClass, "flex items-center gap-1.5")}
              aria-label='Project repository'
            >
              {projectIcon}
              {stars !== null && (
                <div className='flex items-center gap-1 text-xs'>
                  <span>{stars.toLocaleString()}</span>
                </div>
              )}
            </a>
          )}

          {/* 主题切换按钮 */}
          <ThemeToggle />

          {/* 子组件 */}
          {children}
        </div>
      </nav>
    </header>
  );
};

// 简化的 Anchor 组件，用于链接
interface AnchorProps {
  href: string;
  children: React.ReactNode;
  className?: string;
  [key: string]: any;
}

export const Anchor: React.FC<AnchorProps> = ({
  href,
  children,
  className,
  ...props
}) => {
  // 判断是否为外部链接
  const isExternal = href.startsWith("http") || href.startsWith("//");

  if (isExternal) {
    return (
      <a
        href={href}
        target='_blank'
        rel='noopener noreferrer'
        className={className}
        {...props}
      >
        {children}
      </a>
    );
  }

  return (
    <NextLink href={href} className={className} {...props}>
      {children}
    </NextLink>
  );
};
