# Third Party Licenses

This project incorporates source code from the [Kùzu](https://github.com/kuzudb/kuzu) project under the MIT License.

The reused modules include:  
- `antlr4/`: OpenCypher grammar definitions and generated parser code.  
- `binder/`: Binding layer responsible for resolving variable names, schema elements, and functions in queries.  
- `parser/`: Cypher query parser and AST (Abstract Syntax Tree) generation.  
- `catalog/`: Metadata management for schemas, tables, and functions.  
- `common/`: Common utility functions, constants, and data type definitions.  
- `function/`: Built-in function definitions and registration logic.  
- `main/`: Entry point and main application logic integrating various components.  
- `graph/`: Core graph data structures, storage abstractions, and traversal utilities. 
- `planner/`: Logical and physical query planning components.  
- `optimizer/`: Rule-based and heuristic optimization framework.


## Modifications

These modules have been **partially modified** to suit the needs of this project. Major categories of changes include:

- Removing most code related to the execution layer's `processor` and `storage` modules (only retaining interfaces reused by other modules), since NeuG is fully based on a self-developed Hiactor execution engine.  
- Adjusting the `antlr4` grammar to be more compatible with the standard OpenCypher specification, while also supporting additional syntactic sugar.  
- Modifying interfaces in the `catalog` module to adapt to NeuG's own metadata system.  
- Updating interfaces and implementations in the `planner` module to enhance pattern-based CBO (Cost-Based Optimization), introduce subgraph isomorphism algorithms, and further improve CBO efficiency.  
- Adding NeuG-specific RBO (Rule-Based Optimization) rules in the `optimizer` module, such as `FilterPushDownPattern` and `ExpandGetVFusion`.  


## Kùzu License

MIT License

Copyright (c) 2022-2024 Kùzu Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.