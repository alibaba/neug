/* eslint-env node */

import type { Metadata } from "next";
import { Head } from "nextra/components";
import type { FC, ReactNode } from "react";
import "nextra-theme-docs/style.css";

export const metadata: Metadata = {
  description: "x",
  title: {
    absolute: "",
    template: "%s | ",
  },
  other: {
    "msapplication-TileColor": "#fff",
  },
};

type LayoutProps = Readonly<{
  children: ReactNode;
}>;

const RootLayout: FC<LayoutProps> = ({ children }) => {
  return (
    <html suppressHydrationWarning>
      <Head
        // backgroundColor={{
        //   dark: "rgb(15,23,42)",
        //   light: "rgb(254, 252, 232)",
        // }}
        color={{
          hue: { dark: 215, light: 215 },
          saturation: { dark: 95, light: 95 },
        }}
      />
      <body>{children}</body>
    </html>
  );
};

export default RootLayout;
