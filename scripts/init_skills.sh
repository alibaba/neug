#!/bin/bash

# 解析显式参数
OUTPUT=""

# 快捷方式配置：agent_name -> output_directory
# 检查第一个参数是否是快捷方式
case "$1" in
    claude)
        OUTPUT=".claude/skills"
        shift 1
        ;;
    codebuddy)
        OUTPUT=".codebuddy/skills"
        shift 1
        ;;
    codex)
        OUTPUT=".codex/skills"
        shift 1
        ;;
    gemini)
        OUTPUT=".gemini/skills"
        shift 1
        ;;
    kilocode)
        OUTPUT=".kilocode/skills"
        shift 1
        ;;
    opencode)
        OUTPUT=".opencode/skills"
        shift 1
        ;;
    qoder)
        OUTPUT=".qoder/skills"
        shift 1
        ;;
    qwen)
        OUTPUT=".qwen/skills"
        shift 1
        ;;
    roo)
        OUTPUT=".roo/skills"
        shift 1
        ;;
    windsurf)
        OUTPUT=".windsurf/skills"
        shift 1
        ;;
esac

# 解析参数（支持 --key=value 和 --key value 两种格式）
while [[ $# -gt 0 ]]; do
    case $1 in
        --output=*|-o=*)
            OUTPUT="${1#*=}"
            shift
            ;;
        --output|-o)
            OUTPUT="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown parameter '$1'"
            echo "Usage: $0 [shortcut] [--output|-o=<output-path>]"
            echo ""
            echo "Shortcuts:"
            echo "  claude    -> --output=.claude/skills"
            echo "  codebuddy -> --output=.codebuddy/skills"
            echo "  codex     -> --output=.codex/skills"
            echo "  gemini    -> --output=.gemini/skills"
            echo "  kilocode  -> --output=.kilocode/skills"
            echo "  opencode  -> --output=.opencode/skills"
            echo "  qoder     -> --output=.qoder/skills"
            echo "  qwen      -> --output=.qwen/skills"
            echo "  roo       -> --output=.roo/skills"
            echo "  windsurf  -> --output=.windsurf/skills"
            echo ""
            echo "Options:"
            echo "  --output, -o: output directory path"
            exit 1
            ;;
    esac
done

# 检查必需参数
if [ -z "$OUTPUT" ]; then
    echo "Error: --output/-o is required"
    echo "Usage: $0 [shortcut] [--output|-o=<output-path>]"
    echo ""
    echo "Shortcuts:"
    echo "  claude    -> --output=.claude/skills"
    echo "  codebuddy -> --output=.codebuddy/skills"
    echo "  codex     -> --output=.codex/skills"
    echo "  gemini    -> --output=.gemini/skills"
    echo "  kilocode  -> --output=.kilocode/skills"
    echo "  opencode  -> --output=.opencode/skills"
    echo "  qoder     -> --output=.qoder/skills"
    echo "  qwen      -> --output=.qwen/skills"
    echo "  roo       -> --output=.roo/skills"
    echo "  windsurf  -> --output=.windsurf/skills"
    echo ""
    echo "Options:"
    echo "  --output, -o: output directory path"
    exit 1
fi

# 检查源目录是否存在
SOURCE_DIR=".cursor/skills"
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist"
    exit 1
fi

# 创建输出目录（用户提供完整路径）
OUTPUT_DIR="${OUTPUT}"
mkdir -p "$OUTPUT_DIR"

# 复制文件
cp -r "$SOURCE_DIR"/* "$OUTPUT_DIR/"
echo "Copied files from $SOURCE_DIR to $OUTPUT_DIR"

