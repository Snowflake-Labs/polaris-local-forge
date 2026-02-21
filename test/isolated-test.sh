#!/bin/bash
# =============================================================================
# Isolated Test Environment for Polaris Local Forge
# =============================================================================
# Creates a clean test environment that doesn't pollute the source tree.
# All generated files (.env, .kube/, k8s/, etc.) stay in the isolated folder.
#
# Usage:
#   ./scripts/isolated-test.sh              # Create new isolated environment
#   ./scripts/isolated-test.sh setup        # Same as above
#   ./scripts/isolated-test.sh clean        # Remove all isolated test folders
#   ./scripts/isolated-test.sh list         # List existing test folders
#
# Environment variables:
#   TEST_DIR    - Custom test directory (default: /tmp/plf-test-<pid>)
#   SKILL_DIR   - Source directory (auto-detected from script location)
#
# =============================================================================
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# SKILL_DIR: the polaris-local-forge source/skill directory
# Auto-detect from script location
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_DIR="${SKILL_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

# Verify SKILL_DIR is valid
verify_skill_dir() {
    if [ ! -f "$SKILL_DIR/pyproject.toml" ]; then
        echo -e "${RED}Error: SKILL_DIR ($SKILL_DIR) does not contain pyproject.toml${NC}"
        echo ""
        echo "Usage:"
        echo "  # Run from source directory:"
        echo "  ./scripts/isolated-test.sh"
        echo ""
        echo "  # Or set SKILL_DIR explicitly:"
        echo "  export SKILL_DIR=/path/to/polaris-local-forge"
        echo "  ./scripts/isolated-test.sh"
        exit 1
    fi
}

# List existing test folders
list_tests() {
    echo -e "${BLUE}Existing isolated test folders:${NC}"
    echo ""
    ls -la /tmp/plf-test-* 2>/dev/null || echo "  (none found)"
    echo ""
}

# Clean all test folders
clean_tests() {
    echo -e "${YELLOW}Cleaning all isolated test folders...${NC}"
    local count=$(ls -d /tmp/plf-test-* 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -gt 0 ]; then
        rm -rf /tmp/plf-test-*
        echo -e "${GREEN}Removed $count test folder(s)${NC}"
    else
        echo "No test folders found"
    fi
}

# Setup new isolated test environment
setup_test() {
    verify_skill_dir
    
    TEST_DIR="${TEST_DIR:-/tmp/plf-test-$$}"

    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     Polaris Local Forge - Isolated Test Environment            ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "Source directory: ${GREEN}$SKILL_DIR${NC}"
    echo -e "Test directory:   ${GREEN}$TEST_DIR${NC}"
    echo ""

    # Create test directory
    mkdir -p "$TEST_DIR"
    cd "$TEST_DIR"

    # Symlink Taskfile and taskfiles from source
    if [ ! -L "Taskfile.yml" ]; then
        ln -sf "$SKILL_DIR/Taskfile.yml" .
    fi
    if [ ! -L "taskfiles" ]; then
        ln -sf "$SKILL_DIR/taskfiles" .
    fi

    # Initialize the test environment
    echo -e "${YELLOW}=== Initializing test environment ===${NC}"
    uv run --project "$SKILL_DIR" polaris-local-forge init

    echo ""
    echo -e "${YELLOW}=== Running pre-flight checks (informational) ===${NC}"
    # Run doctor but don't fail setup - it's informational
    # User can run 'task doctor --fix' manually if needed
    task doctor || echo -e "${YELLOW}Note: Some checks failed. Run 'task doctor --fix' to resolve.${NC}"

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    Test Environment Ready                       ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Your isolated test environment is ready at:"
    echo -e "  ${BLUE}$TEST_DIR${NC}"
    echo ""
    echo "To use it:"
    echo -e "  ${YELLOW}cd $TEST_DIR${NC}"
    echo ""
    echo "Available commands:"
    echo "  task setup:all     # Full setup (cluster, Polaris, catalog)"
    echo "  task doctor        # Pre-flight checks"
    echo "  task status        # Show cluster status"
    echo "  task verify        # Verify catalog"
    echo "  task teardown      # Clean up cluster"
    echo ""
    echo "Or use the CLI directly:"
    echo "  ./bin/plf doctor"
    echo "  ./bin/plf cluster create"
    echo "  ./bin/plf polaris deploy"
    echo ""
    echo "When done, clean up with:"
    echo "  task teardown"
    echo -e "  ${YELLOW}rm -rf $TEST_DIR${NC}"
    echo ""
    echo "Or clean all test folders:"
    echo -e "  ${YELLOW}$SKILL_DIR/scripts/isolated-test.sh clean${NC}"
    echo ""
}

# Main
case "${1:-setup}" in
    setup)
        setup_test
        ;;
    clean)
        clean_tests
        ;;
    list)
        list_tests
        ;;
    help|--help|-h)
        echo "Usage: $0 [setup|clean|list|help]"
        echo ""
        echo "Commands:"
        echo "  setup   Create a new isolated test environment (default)"
        echo "  clean   Remove all /tmp/plf-test-* folders"
        echo "  list    List existing test folders"
        echo "  help    Show this help"
        echo ""
        echo "Environment variables:"
        echo "  TEST_DIR    Custom test directory (default: /tmp/plf-test-<pid>)"
        echo "  SKILL_DIR   Source directory (auto-detected)"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run '$0 help' for usage"
        exit 1
        ;;
esac
