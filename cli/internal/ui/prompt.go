package ui

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Confirm asks the user a yes/no question. Returns true for yes.
func Confirm(question string) bool {
	fmt.Printf("%s [y/N] ", question)
	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	return answer == "y" || answer == "yes"
}

// Select presents options and returns the chosen index.
func Select(question string, options []string) (int, error) {
	fmt.Println(question)
	for i, opt := range options {
		fmt.Printf("  %s %s\n", DimStyle.Render(fmt.Sprintf("[%d]", i+1)), opt)
	}
	fmt.Print("\nChoice: ")

	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(answer)

	idx, err := strconv.Atoi(answer)
	if err != nil || idx < 1 || idx > len(options) {
		return 0, fmt.Errorf("invalid selection: %q", answer)
	}
	return idx - 1, nil
}
