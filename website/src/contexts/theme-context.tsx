"use client";

import React, { createContext, useContext, useEffect, useState } from "react";

type Theme = "light" | "dark";

interface ThemeContextType {
  theme: Theme;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setTheme] = useState<Theme>("dark");
  const [mounted, setMounted] = useState(false);

  // 在客户端挂载后读取本地存储的主题
  useEffect(() => {
    setMounted(true);
    const savedTheme = localStorage.getItem("neug-theme") as Theme;
    if (savedTheme) {
      setTheme(savedTheme);
      // 立即应用主题到DOM
      const root = document.documentElement;
      root.classList.remove("light", "dark");
      root.classList.add(savedTheme);
    } else {
      // 检查系统偏好
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

  // 当主题改变时，更新DOM类名和本地存储
  useEffect(() => {
    if (!mounted) return;

    const root = document.documentElement;
    root.classList.remove("light", "dark");
    root.classList.add(theme);
    localStorage.setItem("neug-theme", theme);
  }, [theme, mounted]);

  const toggleTheme = () => {
    setTheme((prev) => (prev === "light" ? "dark" : "light"));
  };

  const handleSetTheme = (newTheme: Theme) => {
    setTheme(newTheme);
  };

  // 防止水合不匹配，在客户端挂载前不渲染
  if (!mounted) {
    return <div className='opacity-0'>{children}</div>;
  }

  return (
    <ThemeContext.Provider
      value={{ theme, toggleTheme, setTheme: handleSetTheme }}
    >
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error("useTheme must be used within a ThemeProvider");
  }
  return context;
}
