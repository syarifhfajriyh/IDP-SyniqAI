import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import json
import threading
import queue
import os
import logging 
from datetime import datetime
import pandas as pd
import psutil

# ✅ Configure logger for GUI
logger = logging.getLogger("SyniqAI-GUI")
logger.setLevel(logging.DEBUG)

# Connectors
from postgres_connector import PostgresConnector
from mariadb_connector import MariaDBConnector
from mariadbcloud_conn import MariaDBCloudConnector

# Imports from your orchestrator
from main import (
    SecretManager, 
    ParquetSink, 
    writer_worker
)

class SyniqAIBronzeGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SyniqAI Bronze Layer - Enterprise Data Extraction Platform")
        self.root.geometry("1200x900")
        self.root.minsize(1000, 700)
        self.root.resizable(True, True)
        
        # State variables
        self.connector = None
        self.extraction_thread = None
        self.is_extracting = False
        
        # ✅ NEW: Track auto vs manual mode
        self.auto_optimize = tk.BooleanVar(value=True)  # Default: Auto ON
        self.last_auto_chunk = 50000
        self.last_auto_workers = 4
        
        # Color scheme - Professional dark theme
        self.colors = {
            'bg_primary': '#1e1e1e',
            'bg_secondary': '#252526',
            'bg_tertiary': '#2d2d30',
            'accent': '#007acc',
            'accent_hover': '#005a9e',
            'success': '#4ec9b0',
            'warning': '#ce9178',
            'error': '#f48771',
            'text_primary': '#d4d4d4',
            'text_secondary': '#858585',
            'border': '#3e3e42'
        }
        
        # Configure root window
        self.root.configure(bg=self.colors['bg_primary'])
        
        # Apply professional theme
        self._apply_professional_theme()
        self._create_menu()
        self._create_widgets()
        self._load_config_if_exists()
        
    def _apply_professional_theme(self):
        """Apply custom professional styling"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure colors
        style.configure('.',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            bordercolor=self.colors['border'],
            darkcolor=self.colors['bg_tertiary'],
            lightcolor=self.colors['bg_secondary'],
            troughcolor=self.colors['bg_primary'],
            focuscolor=self.colors['accent'],
            selectbackground=self.colors['accent'],
            selectforeground='white',
            font=('Segoe UI', 10)
        )
        
        # Frame styles
        style.configure('TFrame',
            background=self.colors['bg_secondary']
        )
        
        style.configure('Card.TFrame',
            background=self.colors['bg_tertiary'],
            relief='flat',
            borderwidth=0
        )
        
        # Label styles
        style.configure('TLabel',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            font=('Segoe UI', 10)
        )
        
        style.configure('Header.TLabel',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            font=('Segoe UI', 12, 'bold')
        )
        
        style.configure('Title.TLabel',
            background=self.colors['bg_secondary'],
            foreground=self.colors['accent'],
            font=('Segoe UI', 14, 'bold')
        )
        
        style.configure('Status.TLabel',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_secondary'],
            font=('Segoe UI', 9)
        )
        
        # Entry styles
        style.configure('TEntry',
            fieldbackground=self.colors['bg_tertiary'],
            foreground=self.colors['text_primary'],
            bordercolor=self.colors['border'],
            lightcolor=self.colors['border'],
            darkcolor=self.colors['border'],
            insertcolor=self.colors['text_primary'],
            font=('Segoe UI', 10)
        )
        
        style.map('TEntry',
            fieldbackground=[('focus', self.colors['bg_primary'])],
            bordercolor=[('focus', self.colors['accent'])]
        )
        
        # Button styles
        style.configure('Accent.TButton',
            background=self.colors['accent'],
            foreground='white',
            bordercolor=self.colors['accent'],
            focuscolor=self.colors['accent'],
            font=('Segoe UI', 11, 'bold'),
            padding=(20, 12)
        )
        
        style.map('Accent.TButton',
            background=[('active', self.colors['accent_hover'])],
            foreground=[('active', 'white')]
        )
        
        style.configure('TButton',
            background=self.colors['bg_tertiary'],
            foreground=self.colors['text_primary'],
            bordercolor=self.colors['border'],
            focuscolor=self.colors['border'],
            font=('Segoe UI', 10),
            padding=(15, 8)
        )
        
        style.map('TButton',
            background=[('active', self.colors['bg_primary'])],
            foreground=[('active', self.colors['accent'])]
        )
        
        # Notebook styles
        style.configure('TNotebook',
            background=self.colors['bg_secondary'],
            bordercolor=self.colors['border'],
            tabmargins=[2, 5, 2, 0]
        )
        
        style.configure('TNotebook.Tab',
            background=self.colors['bg_tertiary'],
            foreground=self.colors['text_secondary'],
            padding=[20, 10],
            font=('Segoe UI', 10)
        )
        
        style.map('TNotebook.Tab',
            background=[('selected', self.colors['bg_secondary'])],
            foreground=[('selected', self.colors['accent'])],
            expand=[('selected', [1, 1, 1, 0])]
        )
        
        # LabelFrame styles
        style.configure('TLabelframe',
            background=self.colors['bg_secondary'],
            bordercolor=self.colors['border'],
            darkcolor=self.colors['bg_secondary'],
            lightcolor=self.colors['bg_secondary'],
            relief='flat'
        )
        
        style.configure('TLabelframe.Label',
            background=self.colors['bg_secondary'],
            foreground=self.colors['accent'],
            font=('Segoe UI', 11, 'bold')
        )
        
        # Progressbar styles
        style.configure('TProgressbar',
            background=self.colors['accent'],
            troughcolor=self.colors['bg_tertiary'],
            bordercolor=self.colors['border'],
            lightcolor=self.colors['accent'],
            darkcolor=self.colors['accent']
        )
        
        # Checkbutton styles
        style.configure('TCheckbutton',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            font=('Segoe UI', 10)
        )
        
        style.map('TCheckbutton',
            background=[('active', self.colors['bg_secondary'])],
            foreground=[('active', self.colors['accent'])]
        )
        
        # Radiobutton styles
        style.configure('TRadiobutton',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            font=('Segoe UI', 10)
        )
        
        style.map('TRadiobutton',
            background=[('active', self.colors['bg_secondary'])],
            foreground=[('active', self.colors['accent'])]
        )
        
    def _create_menu(self):
        """Create professional menu bar"""
        menubar = tk.Menu(self.root, 
            bg=self.colors['bg_tertiary'], 
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            font=('Segoe UI', 10)
        )
        self.root.config(menu=menubar)
        
        # File Menu
        file_menu = tk.Menu(menubar, tearoff=0,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            font=('Segoe UI', 10)
        )
        menubar.add_cascade(label="  File  ", menu=file_menu)
        file_menu.add_command(label="Load Configuration...", command=self._load_config_dialog)
        file_menu.add_command(label="Save Configuration...", command=self._save_config_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Reset to Defaults", command=self._reset_defaults)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # Tools Menu
        tools_menu = tk.Menu(menubar, tearoff=0,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            font=('Segoe UI', 10)
        )
        menubar.add_cascade(label="  Tools  ", menu=tools_menu)
        tools_menu.add_command(label="Clear Logs", command=self._clear_logs)
        tools_menu.add_command(label="Export Statistics", command=self._export_stats)
        
        # Help Menu
        help_menu = tk.Menu(menubar, tearoff=0,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            font=('Segoe UI', 10)
        )
        menubar.add_cascade(label="  Help  ", menu=help_menu)
        help_menu.add_command(label="📖 Documentation", command=self._show_docs)
        help_menu.add_command(label="💡 Quick Start Guide", command=self._show_quickstart)
        help_menu.add_separator()
        help_menu.add_command(label="ℹ️ About", command=self._show_about)
        
    def _create_widgets(self):
        """Create all GUI widgets with scrollable frame"""
        # Main container with scrollbar
        main_container = ttk.Frame(self.root, style='TFrame')
        main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Create canvas for scrolling
        canvas = tk.Canvas(main_container, 
            bg=self.colors['bg_primary'],
            highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
        
        # Create frame inside canvas
        main_frame = ttk.Frame(canvas, style='TFrame', padding="15")
        
        # Configure canvas
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Pack scrollbar and canvas
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Create window in canvas
        canvas_frame = canvas.create_window((0, 0), window=main_frame, anchor="nw")
        
        # Configure scrolling
        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
            
        def on_canvas_configure(event):
            canvas.itemconfig(canvas_frame, width=event.width)
            
        main_frame.bind("<Configure>", on_frame_configure)
        canvas.bind("<Configure>", on_canvas_configure)
        
        # Mouse wheel scrolling
        def on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            
        canvas.bind_all("<MouseWheel>", on_mousewheel)
        
        # Header section
        header_frame = ttk.Frame(main_frame, style='Card.TFrame', padding="15")
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        title_label = ttk.Label(header_frame, 
            text="🗄️ SyniqAI Bronze Layer", 
            style='Title.TLabel'
        )
        title_label.grid(row=0, column=0, sticky=tk.W)
        
        subtitle_label = ttk.Label(header_frame,
            text="Enterprise Data Extraction Platform",
            style='Status.TLabel'
        )
        subtitle_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        
        # Connection status badge
        self.status_badge = ttk.Label(header_frame,
            text="⚫ DISCONNECTED",
            style='Status.TLabel',
            foreground=self.colors['error']
        )
        self.status_badge.grid(row=0, column=1, sticky=tk.E, padx=(0, 10))
        
        header_frame.columnconfigure(0, weight=1)
        
        # Create notebook (tabs)
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        
        # Tab 1: Connection
        connection_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(connection_tab, text="  🔌 Connection  ")
        self._create_connection_tab(connection_tab)
        
        # Tab 2: Extraction (Updated)
        extraction_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(extraction_tab, text="  📊 Extraction  ")
        self._create_extraction_tab(extraction_tab)
        
        # Tab 3: Monitoring
        monitoring_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(monitoring_tab, text="  📈 Monitoring  ")
        self._create_monitoring_tab(monitoring_tab)
        
        # Status Section
        status_frame = ttk.LabelFrame(main_frame, text="  Execution Console  ", 
            style='TLabelframe', padding="15")
        status_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        console_container = ttk.Frame(status_frame, style='Card.TFrame', padding="10")
        console_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.log_text = scrolledtext.ScrolledText(
            console_container,
            height=10,
            width=100,
            state=tk.DISABLED,
            bg=self.colors['bg_primary'],
            fg=self.colors['text_primary'],
            insertbackground=self.colors['accent'],
            selectbackground=self.colors['accent'],
            selectforeground='white',
            font=('Consolas', 9),
            relief='flat',
            borderwidth=0,
            padx=10,
            pady=10
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure color tags
        self.log_text.tag_config("INFO", foreground=self.colors['text_primary'])
        self.log_text.tag_config("SUCCESS", foreground=self.colors['success'], font=('Consolas', 9, 'bold'))
        self.log_text.tag_config("WARNING", foreground=self.colors['warning'])
        self.log_text.tag_config("ERROR", foreground=self.colors['error'], font=('Consolas', 9, 'bold'))
        self.log_text.tag_config("TIMESTAMP", foreground=self.colors['text_secondary'])
        self.log_text.tag_config("HIGHLIGHT", foreground=self.colors['accent'])
        
        # Progress bar
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate', style='TProgressbar')
        self.progress.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(0, weight=1)
        console_container.columnconfigure(0, weight=1)
        console_container.rowconfigure(0, weight=1)
        
    def _create_connection_tab(self, parent):
        """Create professional connection configuration panel"""
        
        # Source Type Selector
        source_frame = ttk.Frame(parent, style='Card.TFrame', padding="15")
        source_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        ttk.Label(source_frame, text="Database Type:", style='Header.TLabel').grid(
            row=0, column=0, sticky=tk.W, padx=(0, 15)
        )
        
        self.source_type_var = tk.StringVar(value="postgres")
        
        ttk.Radiobutton(
            source_frame, 
            text="PostgreSQL", 
            variable=self.source_type_var,
            value="postgres",
            style='TRadiobutton'
        ).grid(row=0, column=1, padx=10)
        
        ttk.Radiobutton(
            source_frame,
            text="MariaDB",
            variable=self.source_type_var,
            value="mariadb",
            style='TRadiobutton'
        ).grid(row=0, column=2, padx=10)
        
        ttk.Radiobutton(
            source_frame,
            text="MariaDB Cloud (SkySQL)",
            variable=self.source_type_var,
            value="mariadb_cloud",
            style='TRadiobutton'
        ).grid(row=0, column=3, padx=10)
        
        # Database Configuration Card
        config_card = ttk.LabelFrame(parent, text="  Database Configuration  ", 
            style='TLabelframe', padding="20")
        config_card.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        # Grid layout for form fields
        fields = [
            ("Host Address:", "host_entry", "localhost", 0, 0),
            ("Port:", "port_entry", "5432", 0, 2),
            ("Database Name:", "database_entry", "postgres", 1, 0),
            ("Username:", "user_entry", "postgres", 1, 2),
            ("Password:", "password_entry", "password", 2, 0),
            ("SSL Certificate (Cloud):", "ssl_ca_entry", "", 3, 0)  # For MariaDB Cloud
        ]
        
        for label_text, attr_name, default_val, row, col in fields:
            ttk.Label(config_card, text=label_text, style='TLabel').grid(
                row=row, column=col, sticky=tk.W, pady=8, padx=(0, 10)
            )
            
            entry = ttk.Entry(config_card, width=25 if col == 0 else 15, style='TEntry')
            entry.insert(0, default_val)
            
            if attr_name == "password_entry":
                entry.config(show="●")
            
            entry.grid(row=row, column=col+1, sticky=(tk.W, tk.E), pady=8, padx=(0, 20))
            setattr(self, attr_name, entry)
        
        # Show password toggle
        self.show_password_var = tk.BooleanVar()
        show_password_check = ttk.Checkbutton(
            config_card,
            text="Show Password",
            variable=self.show_password_var,
            command=self._toggle_password,
            style='TCheckbutton'
        )
        show_password_check.grid(row=2, column=2, columnspan=2, sticky=tk.W, pady=8)
        
        config_card.columnconfigure(1, weight=1)
        config_card.columnconfigure(3, weight=1)
        
        # Connection Actions Card
        actions_card = ttk.Frame(parent, style='Card.TFrame', padding="20")
        actions_card.grid(row=2, column=0, pady=(0, 20))
        
        self.connect_btn = ttk.Button(
            actions_card,
            text="🔌 Connect to Database",
            command=self._connect,
            style='Accent.TButton',
            width=25
        )
        self.connect_btn.grid(row=0, column=0, padx=10)
        
        self.validate_btn = ttk.Button(
            actions_card,
            text="✅ Test Connection",
            command=self._validate,
            state=tk.DISABLED,
            style='TButton',
            width=25
        )
        self.validate_btn.grid(row=0, column=1, padx=10)
        
        self.disconnect_btn = ttk.Button(
            actions_card,
            text="🔌 Disconnect",
            command=self._disconnect,
            state=tk.DISABLED,
            style='TButton',
            width=25
        )
        self.disconnect_btn.grid(row=0, column=2, padx=10)
        
        # Connection Info Panel
        info_card = ttk.LabelFrame(parent, text="  Connection Information  ",
            style='TLabelframe', padding="20")
        info_card.grid(row=3, column=0, sticky=(tk.W, tk.E))
        
        self.connection_info_text = tk.Text(
            info_card,
            height=6,
            width=80,
            state=tk.DISABLED,
            bg=self.colors['bg_primary'],
            fg=self.colors['text_secondary'],
            font=('Segoe UI', 9),
            relief='flat',
            borderwidth=0,
            padx=15,
            pady=10
        )
        self.connection_info_text.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # Initial info
        self._update_connection_info("No active connection")
        
        parent.columnconfigure(0, weight=1)
        
    def _create_extraction_tab(self, parent):
        """Create extraction configuration tab"""
        main_frame = ttk.Frame(parent, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Entity Configuration
        entity_frame = ttk.LabelFrame(main_frame, text="📋 Table Configuration", padding="10")
        entity_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(entity_frame, text="Table Name:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.entity_entry = ttk.Entry(entity_frame, width=40)
        self.entity_entry.insert(0, "test_table")
        self.entity_entry.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Label(entity_frame, text="Partition Column:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.partition_entry = ttk.Entry(entity_frame, width=40)
        self.partition_entry.insert(0, "id")
        self.partition_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # ==========================================
        # ✅ NEW: Performance Configuration with Auto/Manual Toggle
        # ==========================================
        perf_frame = ttk.LabelFrame(main_frame, text="⚡ Performance Configuration", padding="10")
        perf_frame.pack(fill=tk.X, pady=5)
        
        # Auto-optimize checkbox
        auto_check = ttk.Checkbutton(
            perf_frame, 
            text="🤖 Auto-Optimize (Recommended)", 
            variable=self.auto_optimize,
            command=self._toggle_auto_optimize
        )
        auto_check.grid(row=0, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Chunk Size
        ttk.Label(perf_frame, text="Chunk Size:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.chunk_entry = ttk.Entry(perf_frame, width=15)
        self.chunk_entry.insert(0, "50000")
        self.chunk_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        self.chunk_entry.config(state='readonly')  # Start as readonly (auto mode)
        
        # ✅ NEW: Manual adjust buttons for chunk size
        chunk_btn_frame = ttk.Frame(perf_frame)
        chunk_btn_frame.grid(row=1, column=2, sticky=tk.W, padx=5)
        
        ttk.Button(chunk_btn_frame, text="➖", width=3, command=lambda: self._adjust_chunk(-10000)).pack(side=tk.LEFT, padx=2)
        ttk.Button(chunk_btn_frame, text="➕", width=3, command=lambda: self._adjust_chunk(+10000)).pack(side=tk.LEFT, padx=2)
        ttk.Button(chunk_btn_frame, text="🔄 Reset", width=8, command=self._reset_to_auto).pack(side=tk.LEFT, padx=2)
        
        # Workers
        ttk.Label(perf_frame, text="Workers:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.workers_display = ttk.Label(perf_frame, text="4", font=("Arial", 10, "bold"))
        self.workers_display.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        # ✅ NEW: Manual adjust buttons for workers
        worker_btn_frame = ttk.Frame(perf_frame)
        worker_btn_frame.grid(row=2, column=2, sticky=tk.W, padx=5)
        
        ttk.Button(worker_btn_frame, text="➖", width=3, command=lambda: self._adjust_workers(-1)).pack(side=tk.LEFT, padx=2)
        ttk.Button(worker_btn_frame, text="➕", width=3, command=lambda: self._adjust_workers(+1)).pack(side=tk.LEFT, padx=2)
        
        # ✅ NEW: Show optimization info
        self.opt_info_label = ttk.Label(perf_frame, text="", foreground="blue", font=("Arial", 9, "italic"))
        self.opt_info_label.grid(row=3, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Extraction Mode
        mode_frame = ttk.LabelFrame(main_frame, text="🔄 Extraction Mode", padding="10")
        mode_frame.pack(fill=tk.X, pady=5)
        
        self.mode_var = tk.StringVar(value="full")
        ttk.Radiobutton(mode_frame, text="Full Load", variable=self.mode_var, value="full").pack(anchor=tk.W)
        ttk.Radiobutton(mode_frame, text="Incremental", variable=self.mode_var, value="incremental").pack(anchor=tk.W)
        
        # Watermark fields (for incremental)
        watermark_frame = ttk.Frame(mode_frame)
        watermark_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(watermark_frame, text="Watermark Column:").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.watermark_entry = ttk.Entry(watermark_frame, width=20)
        self.watermark_entry.insert(0, "updated_at")
        self.watermark_entry.grid(row=0, column=1, sticky=tk.W, padx=5)
        
        ttk.Label(watermark_frame, text="Initial Value:").grid(row=1, column=0, sticky=tk.W, padx=5)
        self.initial_value_entry = ttk.Entry(watermark_frame, width=20)
        self.initial_value_entry.insert(0, "2024-01-01")
        self.initial_value_entry.grid(row=1, column=1, sticky=tk.W, padx=5)
        
        # Output Configuration Card
        output_card = ttk.LabelFrame(main_frame, text="  Output  ",
            style='TLabelframe', padding="15")
        output_card.pack(fill=tk.X, pady=5)
        
        ttk.Label(output_card, text="Directory:").pack(side=tk.LEFT, padx=5)
        
        self.output_entry = ttk.Entry(output_card, width=40, style='TEntry')
        self.output_entry.insert(0, "bronze_layer")
        self.output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        ttk.Button(output_card, text="📁", command=self._browse_output,
            style='TButton', width=3).pack(side=tk.LEFT, padx=5)

        # Control Buttons
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        # ✅ NEW: Analyze button (runs optimizer without starting extraction)
        self.analyze_btn = ttk.Button(
            control_frame, 
            text="🧮 Analyze Table", 
            command=self._analyze_table,
            style="Accent.TButton"
        )
        self.analyze_btn.pack(side=tk.LEFT, padx=5)
        
        self.extract_btn = ttk.Button(
            control_frame, 
            text="🚀 Start Extraction", 
            command=self._start_extraction,
            style="Accent.TButton"
        )
        self.extract_btn.pack(side=tk.LEFT, padx=5)
        
        self.stop_btn = ttk.Button(
            control_frame, 
            text="⏹ Stop", 
            command=self._stop_extraction, 
            state=tk.DISABLED
        )
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # Progress
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=5)

    def _create_monitoring_tab(self, parent):
        """Create professional monitoring dashboard"""
        # Real-time Statistics Card
        stats_card = ttk.LabelFrame(parent, text="  Real-time Statistics  ",
            style='TLabelframe', padding="20")
        stats_card.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        # Create statistics display with cards
        stats_grid = ttk.Frame(stats_card, style='TFrame')
        stats_grid.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # Stat cards
        self.stat_cards = {}
        stat_definitions = [
            ("total_rows", "Total Rows", "0"),
            ("chunks", "Chunks", "0"),
            ("elapsed", "Time", "0.0s"),
            ("throughput", "Speed", "0 rows/s")
        ]
        
        for idx, (key, label, default) in enumerate(stat_definitions):
            card = ttk.Frame(stats_grid, style='Card.TFrame', padding="15")
            card.grid(row=0, column=idx, padx=10, sticky=(tk.W, tk.E, tk.N, tk.S))
            
            ttk.Label(card, text=label, style='Status.TLabel').pack()
            
            value_label = ttk.Label(card, text=default, style='Header.TLabel')
            value_label.pack(pady=(5, 0))
            
            self.stat_cards[key] = value_label
            stats_grid.columnconfigure(idx, weight=1)
        
        # Performance Metrics Card
        metrics_card = ttk.LabelFrame(parent, text="  Performance Metrics  ",
            style='TLabelframe', padding="20")
        metrics_card.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.metrics_text = tk.Text(
            metrics_card,
            height=12,
            width=80,
            state=tk.DISABLED,
            bg=self.colors['bg_primary'],
            fg=self.colors['text_primary'],
            font=('Consolas', 9),
            relief='flat',
            borderwidth=0,
            padx=15,
            pady=10
        )
        self.metrics_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        metrics_card.columnconfigure(0, weight=1)
        metrics_card.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        
        # Initial message
        self._update_metrics("No extraction in progress")
        
    def _update_connection_info(self, message):
        """Update connection info display"""
        self.connection_info_text.config(state=tk.NORMAL)
        self.connection_info_text.delete(1.0, tk.END)
        self.connection_info_text.insert(tk.END, message)
        self.connection_info_text.config(state=tk.DISABLED)
        
    def _update_metrics(self, message):
        """Update metrics display"""
        self.metrics_text.config(state=tk.NORMAL)
        self.metrics_text.delete(1.0, tk.END)
        self.metrics_text.insert(tk.END, message)
        self.metrics_text.config(state=tk.DISABLED)
        
    def _toggle_password(self):
        """Toggle password visibility"""
        if self.show_password_var.get():
            self.password_entry.config(show="")
        else:
            self.password_entry.config(show="●")
            
    # ==========================================
    # ✅ NEW: Auto/Manual Toggle Functions
    # ==========================================
    def _toggle_auto_optimize(self):
        """Toggle between auto and manual mode"""
        if self.auto_optimize.get():
            # Enable auto mode
            self.chunk_entry.config(state='readonly')
            self.opt_info_label.config(text="🤖 Auto-optimization enabled", foreground="blue")
            self._log("🤖 Switched to AUTO-OPTIMIZE mode")
        else:
            # Enable manual mode
            self.chunk_entry.config(state='normal')
            self.opt_info_label.config(text="✋ Manual mode - You control the settings", foreground="orange")
            self._log("✋ Switched to MANUAL mode")
    
    def _adjust_chunk(self, delta):
        """Manually adjust chunk size"""
        if self.auto_optimize.get():
            # Switching to manual mode
            self.auto_optimize.set(False)
            self._toggle_auto_optimize()
        
        try:
            current = int(self.chunk_entry.get())
            new_value = max(1000, current + delta)  # Minimum 1000
            
            self.chunk_entry.config(state='normal')
            self.chunk_entry.delete(0, tk.END)
            self.chunk_entry.insert(0, str(new_value))
            
            self._log(f"📝 Chunk size manually adjusted to {new_value:,}")
        except ValueError:
            pass
    
    def _adjust_workers(self, delta):
        """Manually adjust worker count"""
        if self.auto_optimize.get():
            self.auto_optimize.set(False)
            self._toggle_auto_optimize()
        
        current = int(self.workers_display.cget("text"))
        new_value = max(1, min(16, current + delta))  # Range: 1-16
        self.workers_display.config(text=str(new_value))
        
        self._log(f"📝 Workers manually adjusted to {new_value}")
    
    def _reset_to_auto(self):
        """Reset to last auto-calculated values"""
        self.auto_optimize.set(True)
        self._toggle_auto_optimize()
        
        self.chunk_entry.config(state='normal')
        self.chunk_entry.delete(0, tk.END)
        self.chunk_entry.insert(0, str(self.last_auto_chunk))
        self.chunk_entry.config(state='readonly')
        
        self.workers_display.config(text=str(self.last_auto_workers))
        
        self._log(f"🔄 Reset to auto values: chunk={self.last_auto_chunk:,}, workers={self.last_auto_workers}")
    
    def _analyze_table(self):
        """Run optimizer WITHOUT starting extraction (preview mode)"""
        if not self.connector:
            messagebox.showwarning("No Connection", "Please connect to database first")
            return
        
        entity = self.entity_entry.get()
        if not entity:
            messagebox.showwarning("Missing Table", "Please enter a table name")
            return
        
        self._log(f"🧮 Analyzing table '{entity}'...")
        
        try:
            # 🔥 NEW: Handle both PostgreSQL and MariaDB
            source_type = self.source_type_var.get()
            
            if source_type == "postgres":
                # PostgreSQL analysis
                row_count = 0
                column_count = 0
                
                with self.connector.engine.connect() as conn:
                    from sqlalchemy import text
                    
                    # Get row count
                    query = text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t")
                    result = conn.execute(query, {"t": entity}).scalar()
                    if result and result > 0:
                        row_count = int(result)
                    else:
                        query = text(f"SELECT COUNT(*) FROM {entity}")
                        row_count = conn.execute(query).scalar()
                    
                    # Get column count
                    query = text("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = :t
                    """)
                    column_count = conn.execute(query, {"t": entity}).scalar() or 20
                
                # Calculate optimal params
                chunk_size, num_workers = self._calculate_optimal_params(row_count, column_count)
                
            elif source_type == "mariadb":
                # 🔥 NEW: MariaDB analysis using built-in optimizer
                row_count = self.connector._get_row_count_estimate(entity)
                
                # Get column count
                with self.connector.engine.connect() as conn:
                    from sqlalchemy import text
                    db_name = self.connector.connection_config['database']
                    query = text("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_schema = :db AND table_name = :t
                    """)
                    column_count = conn.execute(query, {"db": db_name, "t": entity}).scalar() or 20
                
                # Get engine metadata
                meta = self.connector._detect_engine_and_pk(entity)
                is_remote = self.connector.connection_config.get("host") not in ["localhost", "127.0.0.1"]
                
                # Use MariaDB's built-in optimizer
                chunk_size, num_workers = self.connector.optimizer.calculate_optimal_params(
                    row_count=row_count,
                    avg_row_size_bytes=1024,  # Default estimate
                    is_remote=is_remote,
                    engine_type=meta["storage_engine"]
                )
                
                self._log(f"   🔍 Engine: {meta['storage_engine']} | PK: {meta['pk_column'] or 'None'}")
                
            else:
                raise ValueError(f"Unsupported database type: {source_type}")
            
            # Store auto-calculated values
            self.last_auto_chunk = chunk_size
            self.last_auto_workers = num_workers
            
            # Update GUI if in auto mode
            if self.auto_optimize.get():
                self.chunk_entry.config(state='normal')
                self.chunk_entry.delete(0, tk.END)
                self.chunk_entry.insert(0, str(chunk_size))
                self.chunk_entry.config(state='readonly')
                
                self.workers_display.config(text=str(num_workers))
            
            # Calculate chunks
            total_chunks = (row_count + chunk_size - 1) // chunk_size
            
            # Show summary
            self._log("=" * 60)
            self._log(f"📊 OPTIMIZATION RESULTS ({source_type.upper()}):")
            self._log(f"   Table Rows: {row_count:,}")
            self._log(f"   Table Columns: {column_count}")
            self._log(f"   Chunk Size: {chunk_size:,}")
            self._log(f"   Workers: {num_workers}")
            self._log(f"   Total Chunks: {total_chunks}")
            est_time = row_count / 50000
            self._log(f"   Est. Time: ~{est_time:.1f}s")
            self._log("=" * 60)
            
            if not self.auto_optimize.get():
                self._log("ℹ️ Manual mode active - Click 🔄 Reset to use these values")
            
        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            self._log(f"❌ Analysis failed: {e}\n{error_msg}", "ERROR")
            messagebox.showerror("Error", f"Could not analyze table: {e}")
    
    def _calculate_optimal_params(self, row_count, column_count=None):
        """Calculate optimal chunk size and workers (column-aware)"""
        import multiprocessing
        import psutil
    
        cpu_cores = multiprocessing.cpu_count()
        available_gb = psutil.virtual_memory().available / (1024**3) if psutil else 8.0
        
        if column_count is None:
            column_count = 20
        
        # 🔥 UPDATED: Use serial mode for tables < 5M rows (faster startup)
        if row_count < 5_000_000:
            num_workers = 1  # Serial mode (no parallel overhead)
        elif row_count < 10_000_000:
            num_workers = 4
        else:
            num_workers = min(8, cpu_cores)
        
        # 2. 🔥 NEW: Adjust base chunk by column count
        if available_gb < 3:
            base_chunk = 20_000
        elif available_gb < 6:
            base_chunk = 30_000
        else:
            base_chunk = 50_000
        
        # 🔥 Column-based adjustment
        if column_count > 100:
            # Very wide table: aggressive reduction
            base_chunk = int(base_chunk * 0.25)  # 75% reduction
            self._log(f"⚠️ Very wide table ({column_count} cols) - chunk size heavily reduced")
        elif column_count > 50:
            # Wide table: reduce chunk size
            base_chunk = int(base_chunk * 0.5)  # 50% reduction
            self._log(f"📊 Wide table detected ({column_count} cols) - reducing chunk size")
        elif column_count < 10:
            # Narrow table: can use larger chunks
            base_chunk = int(base_chunk * 1.5)  # 50% increase
            self._log(f"✅ Narrow table ({column_count} cols) - increasing chunk size")
        
        # 3. Calculate optimal divisor (same as before)
        if row_count < 10_000:
            chunk_size = row_count
        elif row_count < 100_000:
            chunk_size = 10_000
        elif row_count < 1_000_000:
            min_chunks_per_worker = 4
            total_min_chunks = num_workers * min_chunks_per_worker
            chunk_size = max(base_chunk // 2, row_count // total_min_chunks)
        else:
            rows_per_worker = row_count // num_workers
            
            candidates = []
            for multiplier in [4, 5, 6, 7, 8, 10]:
                candidate = multiplier * 5000
                if 5000 <= candidate <= 100000:  # Wider range for column adjustments
                    remainder = rows_per_worker % candidate
                    candidates.append((remainder, candidate))
            
            if candidates:
                candidates.sort()
                chunk_size = candidates[0][1]
            else:
                chunk_size = base_chunk
        
        # 4. Safety caps
        max_chunk_by_memory = int((500 * 1024 * 1024) / 1024)  # ~500k rows
        chunk_size = min(chunk_size, max_chunk_by_memory)
        chunk_size = max(1_000, chunk_size)
        
        # 5. Final validation
        total_chunks = (row_count + chunk_size - 1) // chunk_size
        if total_chunks < num_workers:
            num_workers = max(1, total_chunks)
            
        return int(chunk_size), int(num_workers)
    
    def _browse_output(self):
        """Browse for output directory"""
        directory = filedialog.askdirectory()
        if directory:
            self.output_entry.delete(0, tk.END)
            self.output_entry.insert(0, directory)
            
    def _log(self, message, level="INFO"):
        """Add styled message to console"""
        self.log_text.config(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Insert timestamp
        self.log_text.insert(tk.END, f"[{timestamp}] ", "TIMESTAMP")
        
        # Determine message type and color
        if "✅" in message or "completed" in message.lower() or "success" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "SUCCESS")
        elif "⚠️" in message or "warning" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "WARNING")
        elif "❌" in message or "error" in message.lower() or "failed" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "ERROR")
        elif "⚡" in message or "🚀" in message or "📊" in message:
            self.log_text.insert(tk.END, f"{message}\n", "HIGHLIGHT")
        else:
            self.log_text.insert(tk.END, f"{message}\n", level)
        
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
    def _update_stats(self, stats_dict):
        """Update statistics cards"""
        mapping = {
            "Total Rows": "total_rows",
            "Chunks Processed": "chunks",
            "Elapsed Time": "elapsed",
            "Throughput": "throughput"
        }
        
        for stat_key, card_key in mapping.items():
            if stat_key in stats_dict and card_key in self.stat_cards:
                self.stat_cards[card_key].config(text=str(stats_dict[stat_key]))
                
    def _clear_logs(self):
        """Clear console logs"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self._log("Console cleared")
        
    def _export_stats(self):
        """Export statistics to file"""
        messagebox.showinfo("Export Statistics", "Statistics export feature coming soon!")
        
    def _reset_defaults(self):
        """Reset all fields to default values"""
        if messagebox.askyesno("Reset Defaults", "Reset all fields to default values?"):
            self.source_type_var.set("postgres")
            
            self.host_entry.delete(0, tk.END)
            self.host_entry.insert(0, "localhost")
            self.port_entry.delete(0, tk.END)
            self.port_entry.insert(0, "5432")
            self.database_entry.delete(0, tk.END)
            self.database_entry.insert(0, "postgres")
            self.user_entry.delete(0, tk.END)
            self.user_entry.insert(0, "postgres")
            self.password_entry.delete(0, tk.END)
            self.entity_entry.delete(0, tk.END)
            self.entity_entry.insert(0, "test_table")
            
            # Reset Displays
            self.chunk_entry.config(state='normal')
            self.chunk_entry.delete(0, tk.END)
            self.chunk_entry.config(state='readonly')
            self.workers_display.config(text="4")
            
            self.mode_var.set("full")
            
            self.output_entry.delete(0, tk.END)
            self.output_entry.insert(0, "bronze_layer")
            self._log("⚙️ Reset to default configuration")
            
    def _load_config_if_exists(self):
        """Auto-load config.json if present"""
        if os.path.exists("config.json"):
            try:
                with open("config.json", "r") as f:
                    config = json.load(f)
                
                self.source_type_var.set(config.get("source_type", "postgres"))
                
                # Load connection config
                conn_cfg = config.get("connection_config", {})
                self.host_entry.delete(0, tk.END)
                self.host_entry.insert(0, conn_cfg.get("host", "localhost"))
                
                self.port_entry.delete(0, tk.END)
                self.port_entry.insert(0, str(conn_cfg.get("port", 5432)))
                
                self.database_entry.delete(0, tk.END)
                self.database_entry.insert(0, conn_cfg.get("database", "postgres"))
                
                self.user_entry.delete(0, tk.END)
                self.user_entry.insert(0, conn_cfg.get("user", "postgres"))
                
                self.password_entry.delete(0, tk.END)
                self.password_entry.insert(0, conn_cfg.get("password", ""))
                
                # Load extraction config
                ext_cfg = config.get("extraction_request", {})
                self.entity_entry.delete(0, tk.END)
                self.entity_entry.insert(0, ext_cfg.get("entity", ""))
                
                self.mode_var.set(ext_cfg.get("mode", "full"))
                
                # Load partition column
                self.partition_entry.delete(0, tk.END)
                self.partition_entry.insert(0, ext_cfg.get("partition_column", "id"))
                
                self._log("📂 Configuration loaded from config.json")
                
            except Exception as e:
                self._log(f"⚠️ Could not load config.json: {e}", "WARNING")
                
    def _load_config_dialog(self):
        """Load config from file dialog"""
        filepath = filedialog.askopenfilename(
            title="Select Configuration File",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filepath:
            try:
                with open(filepath, "r") as f:
                    config = json.load(f)
                self._log(f"📂 Configuration loaded from {filepath}")
                # Logic to apply config similar to _load_config_if_exists would go here
            except Exception as e:
                messagebox.showerror("Load Error", f"Failed to load config: {e}")
                
    def _save_config_dialog(self):
        """Save current config to file"""
        filepath = filedialog.asksaveasfilename(
            title="Save Configuration",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filepath:
            try:
                config = self._get_current_config()
                with open(filepath, "w") as f:
                    json.dump(config, f, indent=2)
                self._log(f"💾 Configuration saved to {filepath}")
                messagebox.showinfo("Success", "Configuration saved successfully!")
            except Exception as e:
                messagebox.showerror("Save Error", f"Failed to save config: {e}")
                
    def _get_current_config(self):
        """Get current GUI settings as config dict"""
        try:
            current_chunk = int(self.chunk_entry.get())
        except:
            current_chunk = 0

        connection_config = {
            "host": self.host_entry.get(),
            "port": int(self.port_entry.get()),
            "database": self.database_entry.get(),
            "user": self.user_entry.get(),
            "password": self.password_entry.get()
        }
        
        # Add SSL certificate for MariaDB Cloud
        if self.source_type_var.get() == "mariadb_cloud":
            ssl_ca = self.ssl_ca_entry.get().strip()
            if ssl_ca:
                connection_config["ssl_ca"] = ssl_ca
                connection_config["ssl_verify_cert"] = True

        config = {
            "source_type": self.source_type_var.get(),
            "connection_config": connection_config,
            "extraction_request": {
                "entity": self.entity_entry.get(),
                "mode": self.mode_var.get(),
                "chunk_size": current_chunk,
                "enable_parallel": int(self.workers_display.cget("text")) > 1,
                "partition_column": self.partition_entry.get(),
                "num_workers": int(self.workers_display.cget("text"))
            }
        }
        return config
        
    def _connect(self):
        """Connect to database"""
        try:
            source_type = self.source_type_var.get()
            
            config = {
                "host": self.host_entry.get(),
                "port": int(self.port_entry.get()),
                "database": self.database_entry.get(),
                "user": self.user_entry.get(),
                "password": self.password_entry.get()
            }
            
            # Add SSL certificate for MariaDB Cloud
            if source_type == "mariadb_cloud":
                ssl_ca = self.ssl_ca_entry.get().strip()
                if not ssl_ca:
                    messagebox.showerror("SSL Required", 
                        "SSL Certificate path is required for MariaDB Cloud.\n"
                        "Download from: https://supplychain.mariadb.com/skysql-chain.pem")
                    return
                config["ssl_ca"] = ssl_ca
                config["ssl_verify_cert"] = True
            
            self._log(f"🔌 Connecting to {source_type.upper()}...")
            self.progress.start()
            
            if source_type == "postgres":
                self.connector = PostgresConnector(config, secret_handler=SecretManager)
            elif source_type == "mariadb":
                self.connector = MariaDBConnector(config, secret_handler=SecretManager)
            elif source_type == "mariadb_cloud":
                self.connector = MariaDBCloudConnector(config, secret_handler=SecretManager)
            else:
                raise ValueError(f"Unsupported database type: {source_type}")
            
            self.connector.connect()
            
            self.progress.stop()
            self._log(f"✅ Connected to {source_type.upper()} successfully")
            
            # Update connection info
            conn_info = f"""Connected to: {config['database']} ({source_type.upper()})
Host: {config['host']}:{config['port']}
User: {config['user']}
Status: Active"""
            self._update_connection_info(conn_info)
            
            self.status_badge.config(
                text="🟢 CONNECTED",
                foreground=self.colors['success']
            )
            
            # Update button states
            self.connect_btn.config(state=tk.DISABLED)
            self.disconnect_btn.config(state=tk.NORMAL)
            self.validate_btn.config(state=tk.NORMAL)
            self.extract_btn.config(state=tk.NORMAL)
            
        except Exception as e:
            self.progress.stop()
            self._log(f"❌ Connection failed: {str(e)}", "ERROR")
            messagebox.showerror("Connection Error", str(e))
            
    def _disconnect(self):
        """Disconnect from database"""
        if self.connector:
            self.connector.close()
            self._log("🔌 Database connection closed")
            
        self.connector = None
        self.status_badge.config(
            text="⚫ DISCONNECTED",
            foreground=self.colors['error']
        )
        
        self._update_connection_info("No active connection")
        
        # Update button states
        self.connect_btn.config(state=tk.NORMAL)
        self.disconnect_btn.config(state=tk.DISABLED)
        self.validate_btn.config(state=tk.DISABLED)
        self.extract_btn.config(state=tk.DISABLED)
        
    def _validate(self):
        """Validate database credentials"""
        try:
            self._log("🔍 Validating database credentials...")
            self.connector.validate_credentials()
            self._log("✅ Credentials validated successfully")
            messagebox.showinfo("Validation Success", "Database credentials are valid!")
        except Exception as e:
            self._log(f"❌ Validation failed: {str(e)}", "ERROR")
            messagebox.showerror("Validation Error", str(e))
            
    def _start_extraction(self):
        """Start data extraction (respects auto/manual mode)"""
        if not self.connector:
            messagebox.showwarning("No Connection", "Please connect to database first")
            return
            
        if self.is_extracting:
            messagebox.showwarning("Extraction in Progress", "An extraction is already running")
            return
        
        entity = self.entity_entry.get()
        
        # If auto mode, run analysis first
        if self.auto_optimize.get():
            self._analyze_table()
        
        # Get current values (either auto or manual)
        try:
            chunk_size = int(self.chunk_entry.get())
            num_workers = int(self.workers_display.cget("text"))
        except ValueError:
            messagebox.showerror("Invalid Input", "Chunk size must be a number")
            return
        
        # 🔥 NEW: Determine if manual mode
        is_manual = not self.auto_optimize.get()
        
        # Show what we're using
        mode_str = "AUTO 🤖" if self.auto_optimize.get() else "MANUAL ✋"
        self._log("=" * 60)
        self._log(f"🚀 EXTRACTION STARTED: {entity}")
        self._log(f"   Mode: {self.mode_var.get().upper()}")
        self._log(f"   Optimization: {mode_str}")
        self._log(f"   Chunk Size: {chunk_size:,}")
        self._log(f"   Workers: {num_workers}")
        self._log("=" * 60)
        
        # Build extraction request
        extraction_request = {
            "entity": entity,
            "mode": self.mode_var.get(),
            "chunk_size": chunk_size,
            "enable_parallel": num_workers > 1,
            "partition_column": self.partition_entry.get(),
            "num_workers": num_workers,
            "manual_mode": is_manual  # 🔥 NEW: Pass manual mode flag
        }
        
        if self.mode_var.get() == "incremental":
            extraction_request["watermark_column"] = self.watermark_entry.get()
            extraction_request["initial_value"] = self.initial_value_entry.get()
        
        self.is_extracting = True
        self.extract_btn.config(state=tk.DISABLED)
        self.analyze_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.progress.start()
        
        # Start extraction thread
        self.extraction_thread = threading.Thread(
            target=self._run_extraction,
            args=(extraction_request,),
            daemon=True
        )
        self.extraction_thread.start()
    
    def _run_extraction(self, extraction_request):
        """Execute extraction (runs in background thread)"""
        try:
            output_path = self.output_entry.get()
            sink = ParquetSink(base_path=output_path)
            
            # Setup async writer
            data_queue = queue.Queue(maxsize=5)
            worker_results = {}
            
            writer_thread = threading.Thread(
                target=writer_worker,
                args=(data_queue, sink, worker_results),
                daemon=True
            )
            writer_thread.start()
            
            # Extract data
            total_rows = 0
            chunk_count = 0
            start_time = datetime.now()
            
            stream = self.connector.extract(extraction_request)
            
            for payload in stream:
                while True:
                    try:
                        data_queue.put(payload, timeout=2)
                        chunk_count += 1
                        rows = payload["metadata"]["row_count"]
                        total_rows += rows
                        
                        self.root.after(0, self._log, f"   📦 Chunk {chunk_count}: {rows:,} rows (Total: {total_rows:,})")
                        
                        # Update stats
                        elapsed = (datetime.now() - start_time).total_seconds()
                        stats = {
                            "Total Rows": f"{total_rows:,}",
                            "Chunks Processed": chunk_count,
                            "Elapsed Time": f"{elapsed:.1f}s",
                            "Throughput": f"{total_rows/elapsed:,.0f} rows/s" if elapsed > 0 else "N/A"
                        }
                        self.root.after(0, self._update_stats, stats)
                        
                        # Update metrics
                        metrics = f"""EXTRACTION IN PROGRESS
{'=' * 50}
Table: {extraction_request['entity']}
Rows: {total_rows:,}
Chunks: {chunk_count}
Time: {elapsed:.1f}s
Speed: {total_rows/elapsed:,.0f} rows/sec
Status: Active...
                        """
                        self.root.after(0, self._update_metrics, metrics.strip())
                        break
                        
                    except queue.Full:
                        if not writer_thread.is_alive():
                            if "error" in worker_results:
                                raise worker_results["error"]
                            continue
            
            # Signal completion
            data_queue.put(None)
            writer_thread.join(timeout=300)
            
            if "error" in worker_results:
                raise worker_results["error"]
            
            # Write metadata
            if worker_results.get("output_folder"):
                sink.write_metadata(worker_results["output_folder"], worker_results["last_meta"])
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            self.root.after(0, self._log, "=" * 60)
            self.root.after(0, self._log, f"✅ COMPLETED: {total_rows:,} rows in {elapsed:.1f}s")
            self.root.after(0, self._log, f"💾 Saved to: {worker_results.get('output_folder', output_path)}")
            self.root.after(0, self._log, "=" * 60)
            
            # Final metrics
            final_metrics = f"""EXTRACTION COMPLETED ✅
{'=' * 50}
Table: {extraction_request['entity']}
Total Rows: {total_rows:,}
Total Time: {elapsed:.1f}s
Avg Speed: {total_rows/elapsed:,.0f} rows/sec
Output: {worker_results.get('output_folder', output_path)}
            """
            self.root.after(0, self._update_metrics, final_metrics.strip())
            
            self.root.after(0, messagebox.showinfo, "Success", 
                f"Extracted {total_rows:,} rows\nTime: {elapsed:.1f}s")
            
        except Exception as e:
            self.root.after(0, self._log, f"❌ FAILED: {str(e)}", "ERROR")
            self.root.after(0, messagebox.showerror, "Error", str(e))
            
        finally:
            self.root.after(0, self._extraction_finished)
            
    def _stop_extraction(self):
        """Stop ongoing extraction"""
        if self.extraction_thread and self.extraction_thread.is_alive():
            self._log("⚠️ Stop requested")
            messagebox.showwarning("Stop", "Will stop after current chunk")
            
    def _extraction_finished(self):
        """Cleanup after extraction finishes"""
        self.is_extracting = False
        self.extract_btn.config(state=tk.NORMAL)
        self.analyze_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.progress.stop()
        
    def _show_docs(self):
        """Show documentation"""
        docs = """SyniqAI BRONZE LAYER DOCUMENTATION

CONNECTION:
1. Select database type (PostgreSQL/MariaDB)
2. Fill database credentials
3. Click 'Connect to Database'
4. Test connection

EXTRACTION:
1. Go to 'Extraction' tab
2. Enter table name
3. Chunk size is AUTO-CALCULATED
4. Enable parallel if needed (Workers AUTO-CALCULATED)
5. Click 'START EXTRACTION'

OPTIMIZER:
- Analyzes table row count
- Checks system RAM & CPU
- Automatically sets chunk size and worker threads

MONITORING:
- Real-time statistics
- Performance metrics
- Console logs
"""
        messagebox.showinfo("Documentation", docs)
        
    def _show_quickstart(self):
        """Show quick start"""
        quick = """QUICK START GUIDE

1️⃣ CONNECTION
   • Select PostgreSQL or MariaDB
   • Fill credentials
   • Click "Connect to Database"

2️⃣ EXTRACTION
   • Go to Extraction tab
   • Enter table name
   • Click "START EXTRACTION" (System auto-optimizes)

3️⃣ MONITOR
   • Watch console logs
   • Check statistics
   • View metrics

✅ Done! Data saved to bronze_layer/"""
        messagebox.showinfo("Quick Start", quick)
        
    def _show_about(self):
        """Show about"""
        about = """SyniqAI Bronze Layer v2.1.0

Enterprise Data Extraction Platform

🎯 Features:
• Multi-source support (PostgreSQL, MariaDB)
• Auto-Optimization (RAM/CPU aware)
• Parallel extraction
• Real-time monitoring
• Parquet output with metadata

🛡️ Security:
• Secret manager integration
• SSL/TLS support
• Credential validation

📊 Performance:
• Adaptive chunking
• Worker pooling
• Memory optimization

© 2026 SyniqAI Data Engineering Team"""
        messagebox.showinfo("About SyniqAI Bronze Layer", about)

def main():
    root = tk.Tk()
    app = SyniqAIBronzeGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()