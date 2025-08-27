"use client";

import { Sun, Moon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useState, useEffect } from "react";

export function ThemeToggle() {
  const [mounted, setMounted] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">("dark");

  // 防止水合不匹配
  useEffect(() => {
    setMounted(true);
    // 读取当前主题
    const savedTheme = localStorage.getItem("neug-theme") as "light" | "dark";
    if (savedTheme) {
      setTheme(savedTheme);
      // 立即应用主题到DOM
      const root = document.documentElement;
      root.classList.remove("light", "dark");
      root.classList.add(savedTheme);
      console.log("初始化主题:", savedTheme, "HTML类名:", root.className);
    } else {
      const prefersDark = window.matchMedia(
        "(prefers-color-scheme: dark)"
      ).matches;
      const initialTheme = prefersDark ? "dark" : "light";
      setTheme(initialTheme);
      // 立即应用主题到DOM
      const root = document.documentElement;
      root.classList.remove("light", "dark");
      root.classList.add(initialTheme);
      localStorage.setItem("neug-theme", initialTheme);
    }
  }, []);

  const toggleTheme = () => {
    const newTheme = theme === "light" ? "dark" : "light";
    setTheme(newTheme);

    // 更新DOM和localStorage
    const root = document.documentElement;
    root.classList.remove("light", "dark");
    root.classList.add(newTheme);
    localStorage.setItem("neug-theme", newTheme);

    // 调试信息
    console.log("主题切换到:", newTheme);
    console.log("HTML类名:", root.className);
  };

  if (!mounted) {
    // 服务端渲染时返回一个占位按钮
    return (
      <Button
        variant='ghost'
        size='sm'
        className='w-9 h-9 p-0 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors'
        aria-label='主题切换'
        disabled
      >
        <Sun className='h-4 w-4 text-gray-600 dark:text-gray-400' />
      </Button>
    );
  }

  return (
    <Button
      variant='ghost'
      size='sm'
      onClick={toggleTheme}
      className='w-9 h-9 p-0 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors'
      aria-label={`切换到${theme === "light" ? "深色" : "浅色"}模式`}
    >
      {theme === "light" ? (
        <Moon className='h-4 w-4 text-gray-600 dark:text-gray-400' />
      ) : (
        <Sun className='h-4 w-4 text-gray-600 dark:text-gray-400' />
      )}
    </Button>
  );
}
