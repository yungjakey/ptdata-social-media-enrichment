package examples

import (
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Example model demonstrating different workflows
type workflowModel struct {
	spinner   spinner.Model
	textInput textinput.Model
	viewport  viewport.Model
	workflow  string
	step      int
	loading   bool
	ready     bool
	logs      []string
	width     int
	height    int
}

func newWorkflowModel(workflow string) workflowModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	ti := textinput.New()
	ti.Placeholder = "Enter input..."
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 20

	return workflowModel{
		spinner:   s,
		textInput: ti,
		viewport:  viewport.New(80, 20),
		workflow:  workflow,
		loading:   false,
		logs:      []string{fmt.Sprintf("Starting %s workflow...", workflow)},
	}
}

func (m workflowModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, textinput.Blink)
}

func (m workflowModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "enter":
			if m.textInput.Value() != "" {
				m.loading = true
				m.step++
				switch m.workflow {
				case "development":
					m.logs = append(m.logs, fmt.Sprintf("Development step %d: %s", m.step, m.textInput.Value()))
				case "production":
					m.logs = append(m.logs, fmt.Sprintf("Production step %d: %s", m.step, m.textInput.Value()))
				case "maintenance":
					m.logs = append(m.logs, fmt.Sprintf("Maintenance step %d: %s", m.step, m.textInput.Value()))
				}
				m.textInput.Reset()
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

func (m workflowModel) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	s := fmt.Sprintf("PTData CLI - %s Workflow\n\n", m.workflow)

	if m.loading {
		s += m.spinner.View() + " Processing...\n\n"
	} else {
		s += m.textInput.View() + "\n\n"
	}

	s += m.viewport.View() + "\n\n"
	s += "Press q to quit\n"

	return lipgloss.NewStyle().Margin(1, 2, 0, 2).Render(s)
}

// RunExample starts an example workflow
func RunExample(workflow string) error {
	if workflow == "" {
		workflow = "development" // default workflow
	}

	p := tea.NewProgram(newWorkflowModel(workflow), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %v", err)
	}
	return nil
}
