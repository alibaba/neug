#!/bin/bash

# 解析显式参数
FORMAT=""
OUTPUT=""

# 快捷方式配置：agent_name -> (format, output_directory)
# 检查第一个参数是否是快捷方式
case "$1" in
    amazonq)
        FORMAT="markdown"
        OUTPUT=".amazonq/prompts"
        shift 1
        ;;
    claude)
        FORMAT="markdown"
        OUTPUT=".claude/commands"
        shift 1
        ;;
    codebuddy)
        FORMAT="markdown"
        OUTPUT=".codebuddy/commands"
        shift 1
        ;;
    codex)
        FORMAT="markdown"
        OUTPUT=".codex/commands"
        shift 1
        ;;
    gemini)
        FORMAT="toml"
        OUTPUT=".gemini/commands"
        shift 1
        ;;
    kilocode)
        FORMAT="markdown"
        OUTPUT=".kilocode/rules"
        shift 1
        ;;
    opencode)
        FORMAT="markdown"
        OUTPUT=".opencode/command"
        shift 1
        ;;
    qoder)
        FORMAT="markdown"
        OUTPUT=".qoder/commands"
        shift 1
        ;;
    qwen)
        FORMAT="toml"
        OUTPUT=".qwen/commands"
        shift 1
        ;;
    roo)
        FORMAT="markdown"
        OUTPUT=".roo/rules"
        shift 1
        ;;
    windsurf)
        FORMAT="markdown"
        OUTPUT=".windsurf/workflows"
        shift 1
        ;;
esac

# 解析参数（支持 --key=value 和 --key value 两种格式）
while [[ $# -gt 0 ]]; do
    case $1 in
        --format=*|-f=*)
            FORMAT="${1#*=}"
            shift
            ;;
        --format|-f)
            FORMAT="$2"
            shift 2
            ;;
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
            echo "Usage: $0 [shortcut] [--format|-f=<markdown|toml>] [--output|-o=<output-path>]"
            echo ""
            echo "Shortcuts:"
            echo "  amazonq   -> --format=markdown --output=.amazonq/prompts"
            echo "  claude    -> --format=markdown --output=.claude/commands"
            echo "  codebuddy -> --format=markdown --output=.codebuddy/commands"
            echo "  codex     -> --format=markdown --output=.codex/commands"
            echo "  gemini    -> --format=toml --output=.gemini/commands"
            echo "  kilocode  -> --format=markdown --output=.kilocode/rules"
            echo "  opencode  -> --format=markdown --output=.opencode/command"
            echo "  qoder     -> --format=markdown --output=.qoder/commands"
            echo "  qwen      -> --format=toml --output=.qwen/commands"
            echo "  roo       -> --format=markdown --output=.roo/rules"
            echo "  windsurf  -> --format=markdown --output=.windsurf/workflows"
            echo ""
            echo "Options:"
            echo "  --format, -f: markdown or toml"
            echo "  --output, -o: output directory path"
            exit 1
            ;;
    esac
done

# 检查必需参数
if [ -z "$FORMAT" ] || [ -z "$OUTPUT" ]; then
    echo "Error: Both --format/-f and --output/-o are required"
    echo "Usage: $0 [shortcut] [--format|-f=<markdown|toml>] [--output|-o=<output-path>]"
    echo ""
    echo "Shortcuts:"
    echo "  amazonq   -> --format=markdown --output=.amazonq/prompts"
    echo "  claude    -> --format=markdown --output=.claude/commands"
    echo "  codebuddy -> --format=markdown --output=.codebuddy/commands"
    echo "  codex     -> --format=markdown --output=.codex/commands"
    echo "  gemini    -> --format=toml --output=.gemini/commands"
    echo "  kilocode  -> --format=markdown --output=.kilocode/rules"
    echo "  opencode  -> --format=markdown --output=.opencode/command"
    echo "  qoder     -> --format=markdown --output=.qoder/commands"
    echo "  qwen      -> --format=toml --output=.qwen/commands"
    echo "  roo       -> --format=markdown --output=.roo/rules"
    echo "  windsurf  -> --format=markdown --output=.windsurf/workflows"
    echo ""
    echo "Options:"
    echo "  --format, -f: markdown or toml"
    echo "  --output, -o: output directory path"
    exit 1
fi

# 规范化 format 参数（支持简写）
case "$FORMAT" in
    md|markdown)
        FORMAT="markdown"
        ;;
    toml)
        FORMAT="toml"
        ;;
    *)
        echo "Error: format must be 'markdown' (or 'md') or 'toml'"
        exit 1
        ;;
esac

# 检查源目录是否存在
SOURCE_DIR=".cursor/commands"
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist"
    exit 1
fi

# 创建输出目录（用户提供完整路径）
OUTPUT_DIR="${OUTPUT}"
mkdir -p "$OUTPUT_DIR"

# 复制文件
if [ "$FORMAT" == "markdown" ]; then
    # 直接复制文件
    cp -r "$SOURCE_DIR"/* "$OUTPUT_DIR/"
    echo "Copied files from $SOURCE_DIR to $OUTPUT_DIR"
else
    # toml 格式：复制并转换
    for file in "$SOURCE_DIR"/*.md; do
        if [ ! -f "$file" ]; then
            continue
        fi
        
        filename=$(basename "$file")
        output_file="${OUTPUT_DIR}/${filename%.md}.toml"
        
        # 读取第二行获取 description
        description=""
        second_line=$(sed -n '2p' "$file")
        if [[ "$second_line" =~ ^description:[[:space:]]*(.*)$ ]]; then
            description="${BASH_REMATCH[1]}"
            # 去除首尾空格
            description=$(echo "$description" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        fi
        
        # 读取整个文件内容
        content=$(cat "$file")
        
        # 写入 toml 格式
        {
            echo "description = \"${description}\""
            echo ""
            echo "prompt = \"\"\""
            echo "$content"
            echo "\"\"\""
        } > "$output_file"
        
        echo "Converted: $filename -> ${filename%.md}.toml"
    done
    echo "Converted all files from $SOURCE_DIR to $OUTPUT_DIR (toml format)"
fi

