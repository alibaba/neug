import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  Terminal, 
  Download, 
  GitBranch, 
  Code,
  Database,
  Zap,
  CheckCircle,
  Copy
} from "lucide-react";

export const InstallationSection = () => {
  return (
    <section className="py-24 bg-gray-50 dark:bg-slate-800 transition-colors duration-300">
      <div className="container mx-auto px-6">
        {/* Section Header */}
        <div className="text-center mb-16">
          <Badge variant="outline" className="px-4 py-2 mb-4 border-blue-400/50 bg-blue-500/20 text-blue-600 dark:border-blue-400/30 dark:bg-blue-500/10 dark:text-blue-300">
            <Download className="w-4 h-4 mr-2" />
            Installation Guide
          </Badge>
          <h2 className="text-4xl md:text-5xl font-bold mb-6 text-gray-900 dark:text-white">
            Get Started with
            <br />
            <span className="gradient-text">NeuG</span>
          </h2>
          <p className="text-xl text-gray-600 dark:text-slate-300 max-w-3xl mx-auto font-light">
            Choose your preferred installation method and start using the high-performance graph database in minutes
          </p>
        </div>

        <div className="max-w-4xl mx-auto">
          <Tabs defaultValue="python" className="w-full">
            {/* Installation Methods */}
            <TabsList className="grid w-full grid-cols-2 mb-8 bg-gray-100 dark:bg-slate-700/50 border-gray-200 dark:border-slate-600">
              <TabsTrigger value="python" className="flex items-center gap-2 data-[state=active]:bg-white dark:data-[state=active]:bg-slate-600 data-[state=active]:text-gray-900 dark:data-[state=active]:text-white">
                <Database className="w-4 h-4" />
                Python Installation
              </TabsTrigger>
              <TabsTrigger value="cpp" className="flex items-center gap-2 data-[state=active]:bg-white dark:data-[state=active]:bg-slate-600 data-[state=active]:text-gray-900 dark:data-[state=active]:text-white">
                <Code className="w-4 h-4" />
                C++ Installation
              </TabsTrigger>
            </TabsList>

            <TabsContent value="python" className="space-y-6">
              <Card className="p-8 bg-white dark:bg-slate-700/50 border-gray-200 dark:border-slate-600/50 backdrop-blur-sm">
                <div className="flex items-center gap-3 mb-4">
                  <div className="w-10 h-10 bg-blue-500/10 rounded-xl flex items-center justify-center">
                    <Database className="w-5 h-5 text-blue-500 dark:text-blue-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Install via PyPI</h3>
                    <p className="text-sm text-gray-600 dark:text-slate-400">Recommended method, supports Python 3.8+</p>
                  </div>
                </div>
                
                <div className="bg-gray-50 dark:bg-slate-900/80 border border-gray-200 dark:border-slate-600/50 rounded-lg p-4 font-mono text-sm mb-4">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-gray-700 dark:text-slate-300">
                      <span className="text-blue-500 dark:text-blue-400">$</span> pip install neug
                    </span>
                    <Button variant="ghost" size="sm" className="h-6 w-6 p-0 text-gray-500 dark:text-slate-400 hover:text-gray-700 dark:hover:text-white">
                      <Copy className="w-3 h-3" />
                    </Button>
                  </div>
                  <div className="text-gray-700 dark:text-slate-300 mt-2">
                    <span className="text-blue-500 dark:text-blue-400">$</span> python -c "import neug; print(neug.__version__)"
                  </div>
                </div>

                <div className="flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300">
                  <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                  Supports Linux, macOS, compatible with x86 and ARM architectures
                </div>
              </Card>
            </TabsContent>

            <TabsContent value="cpp" className="space-y-6">
              <Card className="p-8 bg-white dark:bg-slate-700/50 border-gray-200 dark:border-slate-600/50 backdrop-blur-sm">
                <div className="flex items-center gap-3 mb-4">
                  <div className="w-10 h-10 bg-gray-100 dark:bg-slate-700/50 rounded-xl flex items-center justify-center">
                    <Code className="w-5 h-5 text-gray-600 dark:text-slate-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Build from Source</h3>
                    <p className="text-sm text-gray-600 dark:text-slate-400">For C++ developers and advanced users</p>
                  </div>
                </div>
                
                <div className="bg-gray-50 dark:bg-slate-900/80 border border-gray-200 dark:border-slate-600/50 rounded-lg p-4 font-mono text-sm mb-4">
                  <div className="space-y-1 text-gray-700 dark:text-slate-300">
                    <div className="flex items-center justify-between">
                      <span>
                        <span className="text-blue-500 dark:text-blue-400">$</span> git clone https://github.com/GraphScope/neug.git
                      </span>
                      <Button variant="ghost" size="sm" className="h-6 w-6 p-0 text-gray-500 dark:text-slate-400 hover:text-gray-700 dark:hover:text-white">
                        <Copy className="w-3 h-3" />
                      </Button>
                    </div>
                    <span><span className="text-blue-500 dark:text-blue-400">$</span> cd neug &amp;&amp; mkdir build &amp;&amp; cd build</span>
                    <span><span className="text-blue-500 dark:text-blue-400">$</span> cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug</span>
                    <span><span className="text-blue-500 dark:text-blue-400">$</span> make -j$(nproc) &amp;&amp; make install</span>
                  </div>
                </div>

                <div className="flex items-center gap-2 text-sm text-gray-600 dark:text-slate-300">
                  <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                  Get the latest features and contribute to development
                </div>
              </Card>
            </TabsContent>
          </Tabs>

          {/* Quick Start Guide */}
          <div className="mt-16">
            <h3 className="text-2xl font-bold text-center mb-8 text-gray-900 dark:text-white">Quick Installation Verification</h3>
            
            <div className="grid md:grid-cols-2 gap-6">
              {/* Python Quick Start */}
              <Card className="p-6 bg-white dark:bg-slate-700/50 border-gray-200 dark:border-slate-600/50 backdrop-blur-sm">
                <div className="flex items-center gap-3 mb-4">
                  <Badge className="neug-gradient text-white">
                    Recommended
                  </Badge>
                </div>
                
                <div className="flex items-center gap-3 mb-4">
                  <div className="w-10 h-10 bg-blue-500/10 rounded-xl flex items-center justify-center">
                    <Database className="w-5 h-5 text-blue-500 dark:text-blue-400" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-gray-900 dark:text-white">Python Quick Start</h4>
                    <p className="text-sm text-gray-600 dark:text-slate-400">Create your first graph database</p>
                  </div>
                </div>

                <div className="space-y-2 mb-4 text-sm text-gray-600 dark:text-slate-300">
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                    In-memory database mode
                  </div>
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                    Embedded connection
                  </div>
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                    Cypher query support
                  </div>
                </div>

                <div className="bg-gray-50 dark:bg-slate-900/80 border border-gray-200 dark:border-slate-600/50 rounded-lg p-3 font-mono text-sm">
                  <div className="text-gray-700 dark:text-slate-300">
                    <span className="text-blue-500 dark:text-blue-400">import</span> neug<br/>
                    <span className="text-blue-500 dark:text-blue-400">db</span> = neug.Database("")<br/>
                    <span className="text-blue-500 dark:text-blue-400">conn</span> = db.connect()
                  </div>
                </div>
              </Card>

              {/* CLI Tool */}
              <Card className="p-6 bg-white dark:bg-slate-700/50 border-gray-200 dark:border-slate-600/50 backdrop-blur-sm">
                <div className="flex items-center gap-3 mb-4">
                  <div className="w-10 h-10 bg-gray-100 dark:bg-slate-700/50 rounded-xl flex items-center justify-center">
                    <Terminal className="w-5 h-5 text-gray-600 dark:text-slate-400" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-gray-900 dark:text-white">Command Line Tool</h4>
                    <p className="text-sm text-gray-600 dark:text-slate-400">Interactive graph database shell</p>
                  </div>
                </div>

                <div className="space-y-2 mb-4 text-sm text-gray-600 dark:text-slate-300">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-4 h-4 text-gray-600 dark:text-slate-400" />
                    Interactive query interface
                  </div>
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                    Support local and remote connections
                  </div>
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500 dark:text-green-400" />
                    Built-in help and auto-completion
                  </div>
                </div>

                <div className="bg-gray-50 dark:bg-slate-900/80 border border-gray-200 dark:border-slate-600/50 rounded-lg p-3 font-mono text-sm">
                  <div className="text-gray-700 dark:text-slate-300">
                    <span className="text-blue-500 dark:text-blue-400">$</span> neug-cli<br/>
                    <span className="text-green-500 dark:text-green-400">neug&gt;</span> MATCH (n) RETURN count(n)
                  </div>
                </div>
              </Card>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};