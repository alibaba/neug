import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Github,
  Star,
  GitFork,
  Terminal,
  Database,
  Zap,
  Network,
} from "lucide-react";

export const HeroSection = () => {
  return (
    <section className='relative min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 via-white to-gray-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 overflow-hidden transition-colors duration-300'>
      {/* Background Effects */}
      <div className='absolute inset-0 bg-gradient-to-r from-blue-500/10 via-transparent to-blue-500/10 dark:from-blue-500/5 dark:via-transparent dark:to-blue-500/5' />
      <div className='absolute top-1/4 left-1/4 w-96 h-96 bg-blue-500/20 dark:bg-blue-500/10 rounded-full blur-3xl opacity-30' />
      <div className='absolute bottom-1/4 right-1/4 w-80 h-80 bg-blue-500/20 dark:bg-blue-500/10 rounded-full blur-3xl opacity-30' />

      {/* Grid Pattern */}
      <div className='absolute inset-0 bg-[linear-gradient(rgba(0,0,0,0.03)_1px,transparent_1px),linear-gradient(90deg,rgba(0,0,0,0.03)_1px,transparent_1px)] dark:bg-[linear-gradient(rgba(255,255,255,0.02)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.02)_1px,transparent_1px)] bg-[size:50px_50px]' />

      <div className='container pt-24 mx-auto px-6 text-center relative z-10'>
        {/* Badge */}
        <Badge
          variant='outline'
          className='px-4 py-2 mb-8 border-blue-400/50 bg-blue-500/20 text-blue-600 dark:border-blue-400/30 dark:bg-blue-500/10 dark:text-blue-300'
        >
          <Database className='w-4 h-4 mr-2' />
          HTAP Graph Database
        </Badge>

        {/* Main Heading */}
        <h1 className='text-5xl md:text-7xl font-bold mb-6 leading-tight text-gray-900 dark:text-white'>
          <span className='gradient-text'>GraphScope NeuG</span>
        </h1>

        {/* Subheading */}
        <p className='text-xl md:text-2xl text-gray-600 dark:text-slate-300 mb-8 max-w-4xl mx-auto leading-relaxed font-light'>
          High-performance HTAP Graph Database
          <br className='hidden md:block' />
          Designed for embedded graph analytics and real-time transaction
          processing
        </p>

        {/* Features Pills */}
        <div className='flex flex-wrap justify-center gap-4 mb-12 max-md:hidden'>
          <div className='flex items-center gap-2 px-4 py-2 bg-white/80 dark:bg-slate-800/50 border border-blue-400/30 dark:border-blue-400/20 rounded-lg text-sm backdrop-blur-sm'>
            <Database className='w-4 h-4 text-blue-500 dark:text-blue-400' />
            <span className='text-gray-700 dark:text-slate-200'>
              Dual-Mode Architecture
            </span>
          </div>
          <div className='flex items-center gap-2 px-4 py-2 bg-white/80 dark:bg-slate-800/50 border border-gray-300 dark:border-slate-600 rounded-lg text-sm backdrop-blur-sm'>
            <Zap className='w-4 h-4 text-gray-600 dark:text-slate-400' />
            <span className='text-gray-700 dark:text-slate-200'>
              High Performance
            </span>
          </div>
          <div className='flex items-center gap-2 px-4 py-2 bg-white/80 dark:bg-slate-800/50 border border-gray-300 dark:border-slate-600 rounded-lg text-sm backdrop-blur-sm'>
            <Network className='w-4 h-4 text-gray-600 dark:text-slate-400' />
            <span className='text-gray-700 dark:text-slate-200'>
              Cypher Queries
            </span>
          </div>
        </div>

        {/* CTA Buttons */}
        <div className='flex flex-col sm:flex-row gap-4 justify-center items-center mb-16'>
          <Button
            size='lg'
            className='px-8 py-4 text-lg font-semibold neug-gradient hover:neug-gradient-hover text-white border-0 shadow-lg hover:shadow-xl transition-all duration-300 transform hover:scale-105'
          >
            <Terminal className='w-5 h-5 mr-2' />
            Get Started
          </Button>
          <Button
            variant='outline'
            size='lg'
            className='px-8 py-4 text-lg font-semibold border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-100 dark:border-slate-600 dark:text-slate-200 dark:hover:border-slate-500 dark:hover:bg-slate-800/50 transition-all duration-300'
          >
            <Github className='w-5 h-5 mr-2' />
            View Source
          </Button>
        </div>

        {/* Quick Install */}
        <div className='bg-white/80 dark:bg-slate-800/30 border border-gray-200 dark:border-slate-600/50 rounded-xl p-6 max-w-2xl mx-auto backdrop-blur-sm'>
          <div className='flex items-center gap-2 mb-4'>
            <div className='w-3 h-3 rounded-full bg-red-400'></div>
            <div className='w-3 h-3 rounded-full bg-yellow-400'></div>
            <div className='w-3 h-3 rounded-full bg-green-400'></div>
            <span className='text-sm text-gray-600 dark:text-slate-400 ml-2 font-mono'>
              Install NeuG
            </span>
          </div>
          <div className='bg-gray-50 dark:bg-slate-900/80 border border-gray-200 dark:border-slate-700/50 rounded-lg p-4 font-mono text-sm'>
            <div className='text-gray-700 dark:text-slate-300'>
              <span className='text-blue-500 dark:text-blue-400'>$</span>
              <span className='text-gray-800 dark:text-slate-200 ml-2'>
                pip install neug
              </span>
            </div>
            <div className='text-green-500 dark:text-green-400 mt-2 text-xs'>
              ✓ Supports Python 3.8+
            </div>
            <div className='text-gray-700 dark:text-slate-300 mt-3'>
              <span className='text-blue-500 dark:text-blue-400'>$</span>
              <span className='text-gray-800 dark:text-slate-200 ml-2'>
                import neug
              </span>
            </div>
          </div>
        </div>

        {/* Stats */}
        <div className='flex justify-center gap-8 mt-12 text-sm text-gray-600 dark:text-slate-400'>
          <div className='flex items-center gap-2'>
            <Star className='w-4 h-4 text-yellow-500 dark:text-yellow-400' />
            <span>Open Source</span>
          </div>
          <div className='flex items-center gap-2'>
            <Database className='w-4 h-4 text-blue-500 dark:text-blue-400' />
            <span>HTAP Architecture</span>
          </div>
          <div className='flex items-center gap-2'>
            <Zap className='w-4 h-4 text-blue-500 dark:text-blue-400' />
            <span>High Performance</span>
          </div>
        </div>
      </div>
    </section>
  );
};
