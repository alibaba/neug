import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Database,
  Network,
  BarChart3,
  Shield,
  ChevronRight,
  Terminal,
  GitBranch,
  Zap,
} from "lucide-react";

const examples = [
  {
    category: "Graph Data Modeling",
    icon: Database,
    color: "text-blue-400",
    bgColor: "bg-blue-500/10",
    items: [
      "CREATE NODE TABLE Person(id INT64 PRIMARY KEY, name STRING, age INT64)",
      "CREATE REL TABLE KNOWS(FROM Person TO Person, since_year INT64)",
      "CREATE (p:Person {id: 1, name: 'Alice', age: 30})",
      "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name, p2.name",
    ],
  },
  {
    category: "Complex Query Analysis",
    icon: Network,
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
    items: [
      "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) WHERE c.industry = 'Tech'",
      "MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person) RETURN path",
      "MATCH (p:Person) WITH p, size((p)-[:KNOWS]->()) as degree ORDER BY degree DESC",
      "MATCH (p:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(neighbor:Person)",
    ],
  },
  {
    category: "Graph Algorithm Applications",
    icon: BarChart3,
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
    items: [
      "Use PageRank algorithm to calculate node importance",
      "Community detection algorithms to identify user groups",
      "Shortest path analysis and path recommendations",
      "Centrality analysis to find key nodes",
    ],
  },
  {
    category: "Transaction Processing",
    icon: Shield,
    color: "text-gray-600 dark:text-slate-400",
    bgColor: "bg-gray-100 dark:bg-slate-700/50",
    items: [
      "BEGIN TRANSACTION; CREATE (p:Person {id: 100}); COMMIT;",
      "Real-time user relationship updates with consistency guarantees",
      "ACID transaction support for concurrent read-write operations",
      "Data isolation in multi-session environments",
    ],
  },
];

export const UsageExamples = () => {
  return (
    <section className='py-24 bg-gradient-to-b from-gray-100 to-white dark:from-slate-800 dark:to-slate-900 transition-colors duration-300'>
      <div className='container mx-auto px-6'>
        {/* Section Header */}
        <div className='text-center mb-16'>
          <Badge
            variant='outline'
            className='px-4 py-2 mb-4 border-blue-400/50 bg-blue-500/20 text-blue-600 dark:border-blue-400/30 dark:bg-blue-500/10 dark:text-blue-300'
          >
            <Terminal className='w-4 h-4 mr-2' />
            Usage Examples
          </Badge>
          <h2 className='text-4xl md:text-5xl font-bold mb-6 text-gray-900 dark:text-white'>
            <span className='gradient-text'>What Can You Do?</span>
            <br />
            <span className='text-gray-600 dark:text-slate-300 text-3xl md:text-4xl'>
              Real Application Scenarios
            </span>
          </h2>
          <p className='text-xl text-gray-600 dark:text-slate-300 max-w-3xl mx-auto font-light'>
            Explore how developers use NeuG to solve graph data processing challenges and build high-performance graph applications
          </p>
        </div>

        {/* Examples Grid */}
        <div className='grid md:grid-cols-2 gap-8 mb-16'>
          {examples.map((example, index) => (
            <Card
              key={index}
              className='p-8 bg-white dark:bg-slate-800/50 border-gray-200 dark:border-slate-700/50 hover:border-gray-300 dark:hover:border-slate-600/50 transition-all duration-300 hover:shadow-2xl group backdrop-blur-sm'
            >
              <div className='flex items-center gap-4 mb-6'>
                <div
                  className={`w-12 h-12 ${example.bgColor} rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform duration-300`}
                >
                  <example.icon className={`w-6 h-6 ${example.color}`} />
                </div>
                <h3 className='text-xl font-semibold text-gray-900 dark:text-white'>
                  {example.category}
                </h3>
              </div>

              <div className='space-y-3'>
                {example.items.map((item, itemIndex) => (
                  <div
                    key={itemIndex}
                    className='flex items-start gap-3 p-3 bg-gray-50 dark:bg-slate-900/50 rounded-lg border border-gray-200 dark:border-slate-600/30 hover:border-gray-300 dark:hover:border-slate-500/50 transition-colors group/item'
                  >
                    <ChevronRight className='w-4 h-4 text-gray-400 dark:text-slate-500 mt-0.5 group-hover/item:text-blue-500 dark:group-hover/item:text-blue-400 transition-colors flex-shrink-0' />
                    <span className='text-sm text-gray-600 dark:text-slate-300 group-hover/item:text-gray-800 dark:group-hover/item:text-slate-200 transition-colors font-mono leading-relaxed'>
                      {item}
                    </span>
                  </div>
                ))}
              </div>
            </Card>
          ))}
        </div>

        {/* Interactive Demo Section */}
        <div className='bg-gradient-to-r from-gray-100/80 via-white/80 to-gray-100/80 dark:from-slate-800/50 dark:via-slate-700/50 dark:to-slate-800/50 rounded-3xl p-8 md:p-12 border border-gray-200 dark:border-slate-600/50 backdrop-blur-sm'>
          <div className='text-center mb-8'>
            <h3 className='text-3xl font-bold mb-4 text-gray-900 dark:text-white'>
              Try It Now
            </h3>
            <p className='text-lg text-gray-600 dark:text-slate-300 max-w-3xl mx-auto font-light'>
              Experience the powerful capabilities of NeuG through these typical graph database application scenarios
            </p>
          </div>

          <div className='grid md:grid-cols-3 gap-6'>
            <Card className='p-6 bg-white/80 dark:bg-slate-800/30 border-gray-200 dark:border-slate-600/30 hover:border-blue-400/50 dark:hover:border-blue-400/30 transition-colors'>
              <div className='flex items-center gap-3 mb-4'>
                <div className='w-8 h-8 bg-blue-500/20 dark:bg-blue-500/10 rounded-lg flex items-center justify-center'>
                  <Database className='w-4 h-4 text-blue-500 dark:text-blue-400' />
                </div>
                <h4 className='font-semibold text-gray-900 dark:text-white'>
                  Graph Data Modeling
                </h4>
              </div>
              <p className='text-sm text-gray-600 dark:text-slate-300 mb-4'>
                Design efficient graph data models
              </p>
              <Button
                variant='outline'
                size='sm'
                className='w-full border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-100 dark:border-slate-600 dark:text-slate-300 dark:hover:border-slate-500 dark:hover:bg-slate-700/50'
              >
                <Terminal className='w-4 h-4 mr-2' />
                View Examples
              </Button>
            </Card>

            <Card className='p-6 bg-white/80 dark:bg-slate-800/30 border-gray-200 dark:border-slate-600/30 hover:border-gray-300 dark:hover:border-slate-500 transition-colors'>
              <div className='flex items-center gap-3 mb-4'>
                <div className='w-8 h-8 bg-gray-100 dark:bg-slate-700/50 rounded-lg flex items-center justify-center'>
                  <Network className='w-4 h-4 text-gray-600 dark:text-slate-400' />
                </div>
                <h4 className='font-semibold text-gray-900 dark:text-white'>
                  Complex Queries
                </h4>
              </div>
              <p className='text-sm text-gray-600 dark:text-slate-300 mb-4'>
                Execute advanced graph pattern matching
              </p>
              <Button
                variant='outline'
                size='sm'
                className='w-full border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-100 dark:border-slate-600 dark:text-slate-300 dark:hover:border-slate-500 dark:hover:bg-slate-700/50'
              >
                <Terminal className='w-4 h-4 mr-2' />
                View Examples
              </Button>
            </Card>

            <Card className='p-6 bg-white/80 dark:bg-slate-800/30 border-gray-200 dark:border-slate-600/30 hover:border-gray-300 dark:hover:border-slate-500 transition-colors'>
              <div className='flex items-center gap-3 mb-4'>
                <div className='w-8 h-8 bg-gray-100 dark:bg-slate-700/50 rounded-lg flex items-center justify-center'>
                  <BarChart3 className='w-4 h-4 text-gray-600 dark:text-slate-400' />
                </div>
                <h4 className='font-semibold text-gray-900 dark:text-white'>
                  Graph Algorithm Analysis
                </h4>
              </div>
              <p className='text-sm text-gray-600 dark:text-slate-300 mb-4'>
                Run high-performance graph algorithms
              </p>
              <Button
                variant='outline'
                size='sm'
                className='w-full border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-100 dark:border-slate-600 dark:text-slate-300 dark:hover:border-slate-500 dark:hover:bg-slate-700/50'
              >
                <Terminal className='w-4 h-4 mr-2' />
                View Examples
              </Button>
            </Card>
          </div>
        </div>
      </div>
    </section>
  );
};
