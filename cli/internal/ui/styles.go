// Package ui provides terminal styling and rendering utilities.
package ui

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/viper"
)

var (
	// Brand colors
	Primary = lipgloss.Color("12")  // Blue
	Success = lipgloss.Color("10")  // Green
	Warning = lipgloss.Color("11")  // Yellow
	Error   = lipgloss.Color("9")   // Red
	Dim     = lipgloss.Color("8")   // Gray
	White   = lipgloss.Color("15")  // White

	// Text styles
	TitleStyle   = lipgloss.NewStyle().Bold(true).Foreground(Primary)
	ErrorStyle   = lipgloss.NewStyle().Bold(true).Foreground(Error)
	SuccessStyle = lipgloss.NewStyle().Bold(true).Foreground(Success)
	WarningStyle = lipgloss.NewStyle().Bold(true).Foreground(Warning)
	DimStyle     = lipgloss.NewStyle().Foreground(Dim)
	BoldStyle    = lipgloss.NewStyle().Bold(true)
	CodeStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("14"))

	// Status indicators
	CheckMark = SuccessStyle.Render("✓")
	CrossMark = ErrorStyle.Render("✗")
	WarnMark  = WarningStyle.Render("!")
	Arrow     = TitleStyle.Render("→")
)

// ColorEnabled returns whether color output is enabled.
func ColorEnabled() bool {
	if viper.GetBool("no_color") {
		return false
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	return true
}

// Logf prints a formatted status message with a prefix icon.
func Logf(icon, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s %s\n", icon, msg)
}

// Successf prints a green success message.
func Successf(format string, args ...any) {
	Logf(CheckMark, format, args...)
}

// Errorf prints a red error message.
func Errorf(format string, args ...any) {
	Logf(CrossMark, format, args...)
}

// Warnf prints a yellow warning message.
func Warnf(format string, args ...any) {
	Logf(WarnMark, format, args...)
}

// Infof prints a blue info message.
func Infof(format string, args ...any) {
	Logf(Arrow, format, args...)
}

// Header prints a styled header.
func Header(text string) {
	fmt.Println()
	fmt.Println(TitleStyle.Render(text))
	fmt.Println(DimStyle.Render(strings.Repeat("─", len(text)+2)))
}

// KeyValue prints a key-value pair with consistent formatting.
func KeyValue(key, value string) {
	fmt.Printf("  %s %s\n", DimStyle.Render(key+":"), value)
}
