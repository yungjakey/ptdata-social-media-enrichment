package main

import (
	"fmt"
	"os"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type model struct {
	spinner   spinner.Model
	textInput textinput.Model
	viewport  viewport.Model
	err       error
	loading   bool
	ready     bool
	logs      []string
	tableName string
	quitting  bool
	width     int
	height    int
}

func initialModel() model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	ti := textinput.New()
	ti.Placeholder = "Enter table name"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 20

	return model{
		spinner:   s,
		textInput: ti,
		viewport:  viewport.New(80, 20),
		loading:   false,
		logs:      []string{},
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, textinput.Blink)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		case "enter":
			if m.textInput.Value() != "" {
				m.tableName = m.textInput.Value()
				m.loading = true
				m.logs = append(m.logs, fmt.Sprintf("Creating DDL for table: %s", m.tableName))
				return m, m.spinner.Tick
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if !m.ready {
			m.viewport = viewport.New(msg.Width, msg.Height-6)
			m.viewport.YPosition = 0
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - 6
		}
		return m, nil
	}

	m.spinner, cmd = m.spinner.Update(msg)
	cmds = append(cmds, cmd)

	m.textInput, cmd = m.textInput.Update(msg)
	cmds = append(cmds, cmd)

	m.viewport.SetContent(lipgloss.JoinVertical(lipgloss.Left, m.logs...))
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	if m.quitting {
		return "Goodbye!\n"
	}

	if !m.ready {
		return "\n  Initializing..."
	}

	s := "Welcome to PTData CLI\n\n"

	if m.loading {
		s += m.spinner.View() + " Processing...\n\n"
	} else {
		s += m.textInput.View() + "\n\n"
	}

	s += m.viewport.View() + "\n\n"
	s += "Press q to quit\n"

	return lipgloss.NewStyle().Margin(1, 2, 0, 2).Render(s)
}

func main() {
	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running program: %v", err)
		os.Exit(1)
	}
}
