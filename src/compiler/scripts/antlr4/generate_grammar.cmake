# Use a copy of the grammar file and compare since last run.
# This is to make sure clean build from source needn't have Java installed.
# We can't use checksums because of windows line ending normalization.

message(STATUS "Generating ANTLR4 Cypher grammar files...")
find_package(Python3 REQUIRED COMPONENTS Interpreter)

message(STATUS " Regenerating grammar files...")

if(NOT EXISTS antlr4.jar)
    message(INFO " Downloading antlr4.jar")
    file(
        DOWNLOAD https://www.antlr.org/download/antlr-4.13.1-complete.jar antlr4.jar
        EXPECTED_HASH SHA256=bc13a9c57a8dd7d5196888211e5ede657cb64a3ce968608697e4f668251a8487)
endif()

# create the directory for the generated grammar
file(MAKE_DIRECTORY generated)

# Only require Java when we actually need to regenerate (after hash check).
find_package(Java REQUIRED)

# use script to generate final Cypher.g4 file and update tools/shell/include/keywords.h
execute_process(COMMAND ${Python3_EXECUTABLE} keywordhandler.py ${ROOT_DIR}/src/compiler/antlr4/Cypher.g4 ${ROOT_DIR}/src/compiler/antlr4/keywords.txt Cypher.g4 ${ROOT_DIR}/include/neug/compiler/tools/shell/include/keywords.h)

# Generate files.
message(INFO " Generating files...")
execute_process(COMMAND
    ${Java_JAVA_EXECUTABLE} -jar antlr4.jar -Dlanguage=Cpp -no-visitor -no-listener Cypher.g4 -o generated)

# Edit source files.
file(READ generated/CypherLexer.cpp LexerContent)
string(REPLACE "#include \"CypherLexer.h\"" "#include \"cypher_lexer.h\"" LexerReplacedContent "${LexerContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_lexer.cpp "${LexerReplacedContent}")
message(INFO " Generated cypher_lexer.cpp")

file(READ generated/CypherParser.cpp ParserContent)
string(REPLACE "#include \"CypherParser.h\"" "#include \"cypher_parser.h\"" ParserReplacedContent "${ParserContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_parser.cpp "${ParserReplacedContent}")
message(INFO " Generated cypher_parser.cpp")

# Move headers.
file(RENAME generated/CypherParser.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_parser.h)
file(RENAME generated/CypherLexer.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_lexer.h)

# Cleanup
file(REMOVE_RECURSE generated)
message(STATUS "-----Finish generate_grammar.cmake-----")
