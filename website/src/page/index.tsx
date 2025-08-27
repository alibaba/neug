import { HeroSection } from "@/components/hero-section";
import { FeaturesSection } from "@/components/features-section";
import { InstallationSection } from "@/components/installation-section";
import { UsageExamples } from "@/components/usage-examples";
import { CTASection } from "@/components/cta-section";
import { CustomNavbar } from "@/components/custom-navbar";
import { ThemeProvider } from "@/contexts/theme-context";
import "../styles/globals.css";

const Index = () => {
  return (
    <ThemeProvider>
      <div className='min-h-screen bg-gradient-to-br from-gray-50 via-white to-gray-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 transition-colors duration-300'>
        <CustomNavbar
          logo={
            <>
              {/* NeuG Logo */}
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
        />
        <HeroSection />
        <FeaturesSection />
        <InstallationSection />
        <UsageExamples />
        <CTASection />
      </div>
    </ThemeProvider>
  );
};

export default Index;
