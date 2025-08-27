import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Terminal,
  Github,
  Star,
  Database,
  ArrowRight,
  CheckCircle,
  Zap,
} from "lucide-react";

export const CTASection = () => {
  return (
    <section className='py-24 bg-gradient-to-br from-white via-gray-50 to-white dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 relative overflow-hidden transition-colors duration-300'>
      {/* Background Effects */}
      <div className='absolute inset-0 bg-gradient-to-r from-blue-500/10 via-transparent to-blue-500/10 dark:from-blue-500/5 dark:via-transparent dark:to-blue-500/5' />
      <div className='absolute top-0 left-1/2 -translate-x-1/2 w-[800px] h-[400px] bg-gradient-to-b from-blue-500/20 dark:from-blue-500/10 to-transparent rounded-full blur-3xl opacity-30' />

      {/* Grid Pattern */}
      <div className='absolute inset-0 bg-[linear-gradient(rgba(0,0,0,0.03)_1px,transparent_1px),linear-gradient(90deg,rgba(0,0,0,0.03)_1px,transparent_1px)] dark:bg-[linear-gradient(rgba(255,255,255,0.02)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.02)_1px,transparent_1px)] bg-[size:50px_50px]' />

      <div className='container mx-auto px-6 relative z-10'>
        <div className='max-w-4xl mx-auto text-center'>
          {/* Badge */}
          <Badge
            variant='outline'
            className='px-4 py-2 mb-6 border-blue-400/50 bg-blue-500/20 text-blue-600 dark:border-blue-400/30 dark:bg-blue-500/10 dark:text-blue-300 hover:bg-blue-500/30 dark:hover:bg-blue-500/20 transition-all duration-300'
          >
            <Database className='w-4 h-4 mr-2' />
            Ready to start using graph databases?
          </Badge>

          {/* Main Heading */}
          <h2 className='text-4xl md:text-6xl font-bold mb-6 leading-tight text-gray-900 dark:text-white'>
            Build Next-Generation
            <br />
            <span className='gradient-text'>Graph Data Applications</span>
          </h2>

          {/* Description */}
          <p className='text-xl text-gray-600 dark:text-slate-300 mb-12 max-w-3xl mx-auto leading-relaxed font-light'>
            Join the developer community using NeuG and experience the powerful analytical and transactional processing capabilities of high-performance HTAP graph databases
          </p>

          {/* Features List */}
          <div className='flex flex-wrap justify-center gap-6 mb-12'>
            <div className='flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300'>
              <CheckCircle className='w-5 h-5 text-green-500 dark:text-green-400' />
              <span>Open source and free to use</span>
            </div>
            <div className='flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300'>
              <CheckCircle className='w-5 h-5 text-green-500 dark:text-green-400' />
              <span>Apache 2.0 License</span>
            </div>
            <div className='flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300'>
              <CheckCircle className='w-5 h-5 text-green-500 dark:text-green-400' />
              <span>Cross-platform support</span>
            </div>
            <div className='flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300'>
              <CheckCircle className='w-5 h-5 text-green-500 dark:text-green-400' />
              <span>Enterprise-grade performance</span>
            </div>
          </div>

          {/* CTA Buttons */}
          <div className='flex flex-col sm:flex-row gap-4 justify-center items-center mb-16'>
            <Button
              size='lg'
              className='px-8 py-4 text-lg font-semibold neug-gradient hover:neug-gradient-hover text-white border-0 shadow-lg hover:shadow-xl transition-all duration-300 transform hover:scale-105 group'
            >
              <Terminal className='w-5 h-5 mr-2' />
              Get Started Now
              <ArrowRight className='w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform' />
            </Button>

            <Button
              variant='outline'
              size='lg'
              className='px-8 py-4 text-lg font-semibold border-slate-600 text-slate-200 hover:border-slate-500 hover:bg-slate-800/50 transition-all duration-300 group'
            >
              <Github className='w-5 h-5 mr-2' />
              <span>GitHub Repository</span>
              <Star className='w-4 h-4 ml-2 group-hover:fill-current transition-all' />
            </Button>
          </div>

          {/* Stats */}
          <div className='flex justify-center gap-8 mt-12 text-sm text-gray-600 dark:text-slate-400'>
            <div className='text-center'>
              <div className='text-2xl font-bold text-blue-500 dark:text-blue-400 mb-1'>
                HTAP
              </div>
              <div>Hybrid Architecture</div>
            </div>
            <div className='text-center'>
              <div className='text-2xl font-bold text-gray-600 dark:text-slate-400 mb-1'>
                C++
              </div>
              <div>High-Performance Engine</div>
            </div>
            <div className='text-center'>
              <div className='text-2xl font-bold text-gray-600 dark:text-slate-400 mb-1'>
                Cypher
              </div>
              <div>Standard Query Language</div>
            </div>
            <div className='text-center'>
              <div className='text-2xl font-bold text-gray-600 dark:text-slate-400 mb-1'>
                ACID
              </div>
              <div>Transaction Guarantees</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};
