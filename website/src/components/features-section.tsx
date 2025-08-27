import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Database,
  Zap,
  Network,
  Shield,
  Layers,
  BarChart3,
  GitBranch,
  Server,
} from "lucide-react";

const features = [
  {
    icon: Database,
    title: "Dual-Mode Architecture",
    description:
      "Unique embedded mode and service mode, optimized for analytical and transactional workloads respectively, providing optimal performance.",
    color: "text-blue-400",
    bgColor: "bg-blue-500/10",
  },
  {
    icon: Zap,
    title: "High Performance Processing",
    description:
      "Built on modern C++ engine, supporting vectorized operations, multi-core parallel processing, and efficient graph traversal algorithms.",
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
  },
  {
    icon: Network,
    title: "Cypher Query Language",
    description:
      "Supports standard Cypher query language, providing intuitive graph pattern matching and complex graph analysis capabilities.",
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
  },
  {
    icon: Shield,
    title: "ACID Transaction Guarantees",
    description:
      "Provides serializable ACID transaction guarantees, ensuring data consistency and reliability for critical business applications.",
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
  },
  {
    icon: BarChart3,
    title: "Graph Analysis Algorithms",
    description:
      "Built-in rich graph algorithm library, supporting community detection, path analysis, centrality computation, and other advanced graph analysis features.",
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
  },
  {
    icon: Server,
    title: "Cross-Platform Support",
    description: "Supports Linux and macOS, compatible with x86 and ARM architectures, providing Python and C++ interfaces.",
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
  },
];

export const FeaturesSection = () => {
  return (
    <section className='py-24 bg-gradient-to-b from-white to-gray-50 dark:from-slate-900 dark:to-slate-800 transition-colors duration-300'>
      <div className='container mx-auto px-6'>
        {/* Section Header */}
        <div className='text-center mb-16'>
          <Badge
            variant='outline'
            className='px-4 py-2 mb-4 border-blue-400/50 bg-blue-500/20 text-blue-600 dark:border-blue-400/30 dark:bg-blue-500/10 dark:text-blue-300'
          >
            <Layers className='w-4 h-4 mr-2' />
            Core Features
          </Badge>
          <h2 className='text-4xl md:text-5xl font-bold mb-6 text-gray-900 dark:text-white'>
            <span className='gradient-text'>Powerful Graph Database Capabilities</span>
            <br />
            <span className='text-gray-600 dark:text-slate-300 text-3xl md:text-4xl'>
              Built for Modern Applications
            </span>
          </h2>
          <p className='text-xl text-gray-600 dark:text-slate-300 max-w-3xl mx-auto font-light'>
            NeuG combines the advantages of transaction processing and analytical processing, providing you with a high-performance, scalable graph database solution
          </p>
        </div>

        {/* Features Grid */}
        <div className='grid md:grid-cols-2 lg:grid-cols-3 gap-8 mb-16'>
          {features.map((feature, index) => (
            <Card
              key={index}
              className='p-8 bg-white/80 dark:bg-slate-800/50 border-gray-200 dark:border-slate-700/50 hover:border-gray-300 dark:hover:border-slate-600/50 transition-all duration-300 hover:shadow-2xl transform hover:-translate-y-2 group backdrop-blur-sm'
            >
              <div
                className={`w-12 h-12 ${feature.bgColor} rounded-2xl flex items-center justify-center mb-6 group-hover:scale-110 transition-transform duration-300`}
              >
                <feature.icon className={`w-6 h-6 ${feature.color}`} />
              </div>

              <h3 className='text-xl font-semibold mb-4 text-gray-900 dark:text-white'>
                {feature.title}
              </h3>

              <p className='text-gray-600 dark:text-slate-300 leading-relaxed'>
                {feature.description}
              </p>
            </Card>
          ))}
        </div>

        {/* Architecture Highlight */}
        <div className='bg-gradient-to-r from-gray-100/80 via-white/80 to-gray-100/80 dark:from-slate-800/50 dark:via-slate-700/50 dark:to-slate-800/50 rounded-3xl p-8 md:p-12 text-center border border-gray-200 dark:border-slate-600/50 backdrop-blur-sm'>
          <div className='flex justify-center mb-6'>
            <Badge className='px-4 py-2 neug-gradient text-white'>
              <GitBranch className='w-4 h-4 mr-2' />
              HTAP Architecture Advantages
            </Badge>
          </div>

          <h3 className='text-3xl font-bold mb-6 text-gray-900 dark:text-white'>
            One Database, Two Modes
          </h3>

          <p className='text-lg text-gray-600 dark:text-slate-300 mb-8 max-w-3xl mx-auto font-light'>
            NeuG's unique dual-mode design allows you to perform both
            <span className='text-blue-500 dark:text-blue-400 font-semibold'>
              high-performance analytical processing
            </span>
            and
            <span className='text-blue-500 dark:text-blue-400 font-semibold'>
              real-time transaction processing
            </span>
            in the same database
          </p>

          <div className='grid md:grid-cols-2 gap-8 mt-8'>
            <div className='text-center p-6 bg-white/80 dark:bg-slate-800/30 rounded-xl border border-gray-200 dark:border-slate-600/30'>
              <div className='w-16 h-16 bg-blue-500/10 rounded-2xl flex items-center justify-center mx-auto mb-4'>
                <BarChart3 className='w-8 h-8 text-blue-500 dark:text-blue-400' />
              </div>
              <h4 className='text-xl font-semibold text-gray-900 dark:text-white mb-2'>
                Embedded Mode
              </h4>
              <p className='text-gray-600 dark:text-slate-300 text-sm'>
                Optimized for analytical workloads
                <br />
                Batch data loading, complex pattern matching, full-graph computation
              </p>
            </div>
            <div className='text-center p-6 bg-white/80 dark:bg-slate-800/30 rounded-xl border border-gray-200 dark:border-slate-600/30'>
              <div className='w-16 h-16 bg-gray-100 dark:bg-slate-700/50 rounded-2xl flex items-center justify-center mx-auto mb-4'>
                <Server className='w-8 h-8 text-gray-600 dark:text-slate-400' />
              </div>
              <h4 className='text-xl font-semibold text-gray-900 dark:text-white mb-2'>
                Service Mode
              </h4>
              <p className='text-gray-600 dark:text-slate-300 text-sm'>
                Optimized for transactional workloads
                <br />
                Concurrent read-write operations, point queries, multi-session support
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};
