"use client";

import React, { useState, useRef, useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { ChevronDownIcon, GlobeIcon } from "lucide-react";
import { locales } from "../../asset-prefix.mjs";

// 语言配置
const languages_maps = [
  { locale: "en", name: "English", flag: "🇺🇸" },
  { locale: "zh", name: "中文", flag: "🇨🇳" },
  { locale: "de", name: "Deutsch", flag: "🇩🇪" },
  { locale: "fr", name: "Français", flag: "🇫🇷" },
  { locale: "ru", name: "Русский", flag: "🇷🇺" },
  { locale: "ja", name: "日本語", flag: "🇯🇵" },
];

const languages = languages_maps.filter((lang) => {
  return locales.includes(lang.locale);
});

interface LanguageDropdownProps {
  currentLang: string;
}

export const LanguageDropdown: React.FC<LanguageDropdownProps> = ({
  currentLang,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [isMounted, setIsMounted] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const router = useRouter();
  const pathname = usePathname();

  // 客户端渲染标记
  useEffect(() => {
    setIsMounted(true);
  }, []);

  // 点击外部关闭下拉菜单
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [isOpen]);

  // 获取当前语言信息
  const currentLanguage = languages.find((lang) => lang.locale === currentLang);

  // 处理语言切换
  const handleLanguageChange = (newLang: string) => {
    if (!isMounted) return;

    // 构建新的路径
    const pathSegments = pathname.split("/").filter(Boolean);

    // 如果当前路径包含语言代码，替换它
    if (
      pathSegments[0] &&
      languages.some((lang) => lang.locale === pathSegments[0])
    ) {
      pathSegments[0] = newLang;
    } else {
      // 如果没有语言代码，添加到开头
      pathSegments.unshift(newLang);
    }

    const newPath = "/" + pathSegments.join("/");
    router.push(newPath);
    setIsOpen(false);
  };

  // 服务端渲染时返回简单版本
  if (!isMounted) {
    return (
      <div className='flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 dark:text-gray-300 bg-gray-50 dark:bg-gray-800 rounded-md'>
        <GlobeIcon className='w-4 h-4' />
        <span>{currentLanguage?.name || currentLang}</span>
      </div>
    );
  }

  return (
    <div className='relative' ref={dropdownRef}>
      {/* 触发按钮 */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className='flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 dark:text-gray-300 dark:hover:text-gray-100 bg-gray-50 hover:bg-gray-100 dark:bg-gray-800 dark:hover:bg-gray-700 rounded-md transition-colors duration-200 border border-gray-200 dark:border-gray-600'
        aria-label='Select Language'
      >
        <span className='font-medium'>
          {currentLanguage?.flag} {currentLanguage?.name || currentLang}
        </span>
        <ChevronDownIcon
          className={`w-4 h-4 transition-transform duration-200 ${
            isOpen ? "rotate-180" : ""
          }`}
        />
      </button>

      {/* 下拉菜单 */}
      {isOpen && (
        <div className='absolute top-full right-0 mt-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-md shadow-lg z-50 min-w-[160px]'>
          <div className='py-1'>
            {languages.map((language) => (
              <button
                key={language.locale}
                onClick={() => handleLanguageChange(language.locale)}
                className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors duration-150 flex items-center gap-2 ${
                  language.locale === currentLang
                    ? "bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300"
                    : "text-gray-700 dark:text-gray-200"
                }`}
              >
                <span className='text-base'>{language.flag}</span>
                <span className='font-medium'>{language.name}</span>
                {language.locale === currentLang && (
                  <span className='ml-auto text-blue-500 dark:text-blue-400'>
                    ✓
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
