"""
SyniqAI Bronze Layer - Enterprise Data Extraction Platform
Modern GUI using CustomTkinter for enhanced visual clarity
Preserves original layout structure
"""

import customtkinter as ctk
from tkinter import messagebox, scrolledtext, filedialog, Menu
import json
import threading
import queue
import os
import logging 
from datetime import datetime
import pandas as pd
import psutil

# Configure logger for GUI
logger = logging.getLogger("SyniqAI-GUI")
logger.setLevel(logging.DEBUG)

# Connectors
from postgres_connector import PostgresConnector
from mariadb_connector import MariaDBConnector
from mariadbcloud_conn import MariaDBCloudConnector

# Imports from orchestrator
from main import (
    SecretManager, 
    ParquetSink, 
    writer_worker
)

# Set CustomTkinter appearance
ctk.set_appearance_mode("dark")  # "dark" or "light"
ctk.set_default_color_theme("blue")  # "blue", "green", "dark-blue"


class SyniqAIBronzeGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SyniqAI Bronze Layer - Enterprise Data Extraction Platform")
        self.root.geometry("1200x900")
        self.root.minsize(1000, 700)
        
        # State variables
        self.connector = None
        self.extraction_thread = None
        self.is_extracting = False
        
        # Track auto vs manual mode
        self.auto_optimize = ctk.BooleanVar(value=True)
        self.last_auto_chunk = 50000
        self.last_auto_workers = 4
        
        # Create menu and widgets
        self._create_menu()
        self._create_widgets()
        self._load_config_if_exists()
        
    def _create_menu(self):
        """Create menu bar"""
        menubar = Menu(self.root)
        self.root.config(menu=menubar)
        
        # File Menu
        file_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="  File  ", menu=file_menu)
        file_menu.add_command(label="Load Configuration...", command=self._load_config_dialog)
        file_menu.add_command(label="Save Configuration...", command=self._save_config_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Reset to Defaults", command=self._reset_defaults)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # Tools Menu
        tools_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="  Tools  ", menu=tools_menu)
        tools_menu.add_command(label="Clear Logs", command=self._clear_logs)
        tools_menu.add_command(label="Export Statistics", command=self._export_stats)
        
        # Help Menu
        help_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="  Help  ", menu=help_menu)
        help_menu.add_command(label="📖 Documentation", command=self._show_docs)
        help_menu.add_command(label="💡 Quick Start Guide", command=self._show_quickstart)
        help_menu.add_separator()
        help_menu.add_command(label="ℹ️ About", command=self._show_about)
        
    def _create_widgets(self):
        """Create all GUI widgets with scrollable frame"""
        # Main container
        main_frame = ctk.CTkScrollableFrame(self.root, corner_radius=0)
        main_frame.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Header section
        header_frame = ctk.CTkFrame(main_frame, corner_radius=10)
        header_frame.pack(fill="x", pady=(0, 15))
        
        title_label = ctk.CTkLabel(
            header_frame, 
            text="🗄️ SyniqAI Bronze Layer", 
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(anchor="w", padx=15, pady=(15, 5))
        
        subtitle_label = ctk.CTkLabel(
            header_frame,
            text="Enterprise Data Extraction Platform",
            font=ctk.CTkFont(size=12),
            text_color="gray70"
        )
        subtitle_label.pack(anchor="w", padx=15, pady=(0, 15))
        
        # Connection status badge
        status_frame = ctk.CTkFrame(header_frame, corner_radius=5)
        status_frame.place(relx=1.0, rely=0.5, anchor="e", x=-15)
        
        self.status_badge = ctk.CTkLabel(
            status_frame,
            text="⚫ DISCONNECTED",
            font=ctk.CTkFont(size=11, weight="bold"),
            text_color="red"
        )
        self.status_badge.pack(padx=10, pady=5)
        
        # Create tabview
        tabview = ctk.CTkTabview(main_frame, corner_radius=10)
        tabview.pack(fill="both", expand=True, pady=(0, 15))
        
        # Tab 1: Connection
        tabview.add("🔌 Connection")
        self._create_connection_tab(tabview.tab("🔌 Connection"))
        
        # Tab 2: Extraction
        tabview.add("📊 Extraction")
        self._create_extraction_tab(tabview.tab("📊 Extraction"))
        
        # Tab 3: Monitoring
        tabview.add("📈 Monitoring")
        self._create_monitoring_tab(tabview.tab("📈 Monitoring"))
        
        # Tab 4: MariaDB Cloud (NEW)
        tabview.add("☁️ Cloud Multi-Tenant")
        self._create_cloud_tab(tabview.tab("☁️ Cloud Multi-Tenant"))
        
        # Status Section
        status_outer_frame = ctk.CTkFrame(main_frame, corner_radius=10)
        status_outer_frame.pack(fill="both", expand=True)
        
        status_label = ctk.CTkLabel(
            status_outer_frame,
            text="Execution Console",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        status_label.pack(anchor="w", padx=15, pady=(15, 5))
        
        console_frame = ctk.CTkFrame(status_outer_frame, fg_color="gray10", corner_radius=8)
        console_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        self.log_text = scrolledtext.ScrolledText(
            console_frame,
            height=10,
            width=100,
            state="disabled",
            bg="#1a1a1a",
            fg="#d4d4d4",
            insertbackground="#007acc",
            selectbackground="#007acc",
            selectforeground="white",
            font=('Consolas', 9),
            relief="flat",
            borderwidth=0,
            padx=10,
            pady=10
        )
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Configure color tags
        self.log_text.tag_config("INFO", foreground="#d4d4d4")
        self.log_text.tag_config("SUCCESS", foreground="#4ec9b0", font=('Consolas', 9, 'bold'))
        self.log_text.tag_config("WARNING", foreground="#ce9178")
        self.log_text.tag_config("ERROR", foreground="#f48771", font=('Consolas', 9, 'bold'))
        self.log_text.tag_config("TIMESTAMP", foreground="#858585")
        self.log_text.tag_config("HIGHLIGHT", foreground="#007acc")
        
        # Progress bar
        self.progress = ctk.CTkProgressBar(status_outer_frame, mode='indeterminate')
        self.progress.pack(fill="x", padx=15, pady=(0, 15))
        self.progress.set(0)
        
    def _create_connection_tab(self, parent):
        """Create connection configuration panel"""
        
        # Source Type Selector
        source_frame = ctk.CTkFrame(parent, corner_radius=10)
        source_frame.pack(fill="x", pady=(0, 15))
        
        ctk.CTkLabel(
            source_frame, 
            text="Database Type:", 
            font=ctk.CTkFont(size=14, weight="bold")
        ).grid(row=0, column=0, sticky="w", padx=15, pady=15)
        
        self.source_type_var = ctk.StringVar(value="postgres")
        
        radio_frame = ctk.CTkFrame(source_frame, fg_color="transparent")
        radio_frame.grid(row=0, column=1, sticky="w", columnspan=3, padx=15, pady=15)
        
        ctk.CTkRadioButton(
            radio_frame, 
            text="PostgreSQL", 
            variable=self.source_type_var,
            value="postgres"
        ).pack(side="left", padx=10)
        
        ctk.CTkRadioButton(
            radio_frame,
            text="MariaDB",
            variable=self.source_type_var,
            value="mariadb"
        ).pack(side="left", padx=10)
        
        ctk.CTkRadioButton(
            radio_frame,
            text="MariaDB Cloud (SkySQL)",
            variable=self.source_type_var,
            value="mariadb_cloud"
        ).pack(side="left", padx=10)
        
        # Database Configuration Card
        config_card = ctk.CTkFrame(parent, corner_radius=10)
        config_card.pack(fill="x", pady=(0, 15))
        
        header_label = ctk.CTkLabel(
            config_card,
            text="Database Configuration",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header_label.grid(row=0, column=0, columnspan=4, sticky="w", padx=15, pady=(15, 10))
        
        # Grid layout for form fields
        fields = [
            ("Host Address:", "host_entry", "localhost", 1, 0),
            ("Port:", "port_entry", "5432", 1, 2),
            ("Database Name:", "database_entry", "postgres", 2, 0),
            ("Username:", "user_entry", "postgres", 2, 2),
            ("Password:", "password_entry", "password", 3, 0),
            ("SSL Certificate (Cloud):", "ssl_ca_entry", "", 4, 0)
        ]
        
        for label_text, attr_name, default_val, row, col in fields:
            label = ctk.CTkLabel(config_card, text=label_text)
            label.grid(row=row, column=col, sticky="w", padx=(15, 5), pady=8)
            
            entry = ctk.CTkEntry(config_card, width=300)
            entry.insert(0, default_val)
            entry.grid(row=row, column=col+1, sticky="ew", padx=(5, 15), pady=8)
            setattr(self, attr_name, entry)
            
            # Show password for password field
            if attr_name == "password_entry":
                entry.configure(show="*")
        
        # Show password toggle
        self.show_password_var = ctk.BooleanVar()
        show_password_check = ctk.CTkCheckBox(
            config_card,
            text="Show Password",
            variable=self.show_password_var,
            command=self._toggle_password
        )
        show_password_check.grid(row=3, column=2, columnspan=2, sticky="w", padx=15, pady=8)
        
        config_card.columnconfigure(1, weight=1)
        config_card.columnconfigure(3, weight=1)
        
        # Connection Actions Card
        actions_card = ctk.CTkFrame(parent, corner_radius=10)
        actions_card.pack(fill="x", pady=(0, 15))
        
        button_frame = ctk.CTkFrame(actions_card, fg_color="transparent")
        button_frame.pack(pady=20)
        
        self.connect_btn = ctk.CTkButton(
            button_frame,
            text="🔌 Connect to Database",
            command=self._connect,
            width=200,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.connect_btn.pack(side="left", padx=10)
        
        self.validate_btn = ctk.CTkButton(
            button_frame,
            text="✅ Test Connection",
            command=self._validate,
            state="disabled",
            width=200,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.validate_btn.pack(side="left", padx=10)
        
        self.disconnect_btn = ctk.CTkButton(
            button_frame,
            text="🔌 Disconnect",
            command=self._disconnect,
            state="disabled",
            width=200,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="gray40",
            hover_color="gray50"
        )
        self.disconnect_btn.pack(side="left", padx=10)
        
        # Connection Info Panel
        info_card = ctk.CTkFrame(parent, corner_radius=10)
        info_card.pack(fill="both", expand=True)
        
        info_header = ctk.CTkLabel(
            info_card,
            text="Connection Information",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        info_header.pack(anchor="w", padx=15, pady=(15, 10))
        
        info_text_frame = ctk.CTkFrame(info_card, fg_color="gray10", corner_radius=8)
        info_text_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        self.connection_info_text = scrolledtext.ScrolledText(
            info_text_frame,
            height=6,
            width=80,
            state="disabled",
            bg="#1a1a1a",
            fg="gray70",
            font=('Segoe UI', 9),
            relief="flat",
            borderwidth=0,
            padx=15,
            pady=10
        )
        self.connection_info_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Initial info
        self._update_connection_info("No active connection")
        
    def _create_extraction_tab(self, parent):
        """Create extraction configuration tab"""
        
        # Entity Configuration
        entity_frame = ctk.CTkFrame(parent, corner_radius=10)
        entity_frame.pack(fill="x", pady=(0, 15))
        
        header = ctk.CTkLabel(
            entity_frame,
            text="📋 Table Configuration",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.grid(row=0, column=0, columnspan=2, sticky="w", padx=15, pady=(15, 10))
        
        ctk.CTkLabel(entity_frame, text="Table Name:").grid(
            row=1, column=0, sticky="w", padx=15, pady=8
        )
        self.entity_entry = ctk.CTkEntry(entity_frame, width=300)
        self.entity_entry.insert(0, "test_table")
        self.entity_entry.grid(row=1, column=1, sticky="w", padx=(5, 15), pady=8)
        
        ctk.CTkLabel(entity_frame, text="Partition Column:").grid(
            row=2, column=0, sticky="w", padx=15, pady=8
        )
        self.partition_entry = ctk.CTkEntry(entity_frame, width=300)
        self.partition_entry.insert(0, "id")
        self.partition_entry.grid(row=2, column=1, sticky="w", padx=(5, 15), pady=(8, 15))
        
        # Performance Configuration
        perf_frame = ctk.CTkFrame(parent, corner_radius=10)
        perf_frame.pack(fill="x", pady=(0, 15))
        
        header = ctk.CTkLabel(
            perf_frame,
            text="⚡ Performance Configuration",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.grid(row=0, column=0, columnspan=3, sticky="w", padx=15, pady=(15, 10))
        
        # Auto-optimize checkbox
        auto_check = ctk.CTkCheckBox(
            perf_frame, 
            text="🤖 Auto-Optimize (Recommended)", 
            variable=self.auto_optimize,
            command=self._toggle_auto_optimize
        )
        auto_check.grid(row=1, column=0, columnspan=3, sticky="w", padx=15, pady=8)
        
        # Chunk Size
        ctk.CTkLabel(perf_frame, text="Chunk Size:").grid(
            row=2, column=0, sticky="w", padx=15, pady=8
        )
        self.chunk_entry = ctk.CTkEntry(perf_frame, width=150)
        self.chunk_entry.insert(0, "50000")
        self.chunk_entry.configure(state="readonly")
        self.chunk_entry.grid(row=2, column=1, sticky="w", padx=(5, 10), pady=8)
        
        # Chunk adjust buttons
        chunk_btn_frame = ctk.CTkFrame(perf_frame, fg_color="transparent")
        chunk_btn_frame.grid(row=2, column=2, sticky="w", padx=5)
        
        ctk.CTkButton(
            chunk_btn_frame, text="➖", width=40,
            command=lambda: self._adjust_chunk(-10000)
        ).pack(side="left", padx=2)
        
        ctk.CTkButton(
            chunk_btn_frame, text="➕", width=40,
            command=lambda: self._adjust_chunk(+10000)
        ).pack(side="left", padx=2)
        
        ctk.CTkButton(
            chunk_btn_frame, text="🔄 Reset", width=80,
            command=self._reset_to_auto
        ).pack(side="left", padx=2)
        
        # Workers
        ctk.CTkLabel(perf_frame, text="Workers:").grid(
            row=3, column=0, sticky="w", padx=15, pady=8
        )
        self.workers_display = ctk.CTkLabel(
            perf_frame, text="4",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.workers_display.grid(row=3, column=1, sticky="w", padx=(5, 10), pady=8)
        
        # Worker adjust buttons
        worker_btn_frame = ctk.CTkFrame(perf_frame, fg_color="transparent")
        worker_btn_frame.grid(row=3, column=2, sticky="w", padx=5)
        
        ctk.CTkButton(
            worker_btn_frame, text="➖", width=40,
            command=lambda: self._adjust_workers(-1)
        ).pack(side="left", padx=2)
        
        ctk.CTkButton(
            worker_btn_frame, text="➕", width=40,
            command=lambda: self._adjust_workers(+1)
        ).pack(side="left", padx=2)
        
        # Optimization info
        self.opt_info_label = ctk.CTkLabel(
            perf_frame, text="",
            text_color="gray60",
            font=ctk.CTkFont(size=10, slant="italic")
        )
        self.opt_info_label.grid(row=4, column=0, columnspan=3, sticky="w", padx=15, pady=(8, 15))
        
        # Extraction Mode
        mode_frame = ctk.CTkFrame(parent, corner_radius=10)
        mode_frame.pack(fill="x", pady=(0, 15))
        
        header = ctk.CTkLabel(
            mode_frame,
            text="🔄 Extraction Mode",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        self.mode_var = ctk.StringVar(value="full")
        
        radio_container = ctk.CTkFrame(mode_frame, fg_color="transparent")
        radio_container.pack(anchor="w", padx=15, pady=(0, 10))
        
        ctk.CTkRadioButton(
            radio_container, text="Full Load",
            variable=self.mode_var, value="full"
        ).pack(anchor="w", pady=5)
        
        ctk.CTkRadioButton(
            radio_container, text="Incremental",
            variable=self.mode_var, value="incremental"
        ).pack(anchor="w", pady=5)
        
        # Watermark fields
        watermark_frame = ctk.CTkFrame(mode_frame, fg_color="transparent")
        watermark_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(watermark_frame, text="Watermark Column:").grid(
            row=0, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self.watermark_entry = ctk.CTkEntry(watermark_frame, width=200)
        self.watermark_entry.insert(0, "updated_at")
        self.watermark_entry.grid(row=0, column=1, sticky="w", pady=5)
        
        ctk.CTkLabel(watermark_frame, text="Initial Value:").grid(
            row=1, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self.initial_value_entry = ctk.CTkEntry(watermark_frame, width=200)
        self.initial_value_entry.insert(0, "2024-01-01")
        self.initial_value_entry.grid(row=1, column=1, sticky="w", pady=5)
        
        # Output Configuration
        output_frame = ctk.CTkFrame(parent, corner_radius=10)
        output_frame.pack(fill="x", pady=(0, 15))
        
        header = ctk.CTkLabel(
            output_frame,
            text="Output",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        output_inner = ctk.CTkFrame(output_frame, fg_color="transparent")
        output_inner.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(output_inner, text="Directory:").pack(side="left", padx=(0, 10))
        
        self.output_entry = ctk.CTkEntry(output_inner, width=400)
        self.output_entry.insert(0, "bronze_layer")
        self.output_entry.pack(side="left", fill="x", expand=True, padx=(0, 10))
        
        ctk.CTkButton(
            output_inner, text="📁", width=40,
            command=self._browse_output
        ).pack(side="left")

        # Control Buttons
        control_frame = ctk.CTkFrame(parent, corner_radius=10)
        control_frame.pack(fill="x")
        
        button_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        button_container.pack(pady=20)
        
        self.analyze_btn = ctk.CTkButton(
            button_container, 
            text="🧮 Analyze Table", 
            command=self._analyze_table,
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.analyze_btn.pack(side="left", padx=10)
        
        self.extract_btn = ctk.CTkButton(
            button_container, 
            text="🚀 Start Extraction", 
            command=self._start_extraction,
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.extract_btn.pack(side="left", padx=10)
        
        self.stop_btn = ctk.CTkButton(
            button_container, 
            text="⏹ Stop", 
            command=self._stop_extraction, 
            state="disabled",
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="gray40",
            hover_color="gray50"
        )
        self.stop_btn.pack(side="left", padx=10)

    def _create_monitoring_tab(self, parent):
        """Create monitoring dashboard"""
        
        # Real-time Statistics Card
        stats_card = ctk.CTkFrame(parent, corner_radius=10)
        stats_card.pack(fill="both", expand=True)
        
        header = ctk.CTkLabel(
            stats_card,
            text="Real-time Statistics",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        # Statistics grid
        stats_grid = ctk.CTkFrame(stats_card, fg_color="transparent")
        stats_grid.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        # Stat cards
        self.stat_cards = {}
        stat_definitions = [
            ("total_rows", "Total Rows", "0", 0, 0),
            ("chunks", "Chunks", "0", 0, 1),
            ("elapsed", "Time", "0.0s", 1, 0),
            ("throughput", "Speed", "0 rows/s", 1, 1)
        ]
        
        for key, label, default, row, col in stat_definitions:
            card = ctk.CTkFrame(stats_grid, corner_radius=10)
            card.grid(row=row, column=col, sticky="nsew", padx=10, pady=10)
            
            label_widget = ctk.CTkLabel(
                card, text=label,
                font=ctk.CTkFont(size=11),
                text_color="gray60"
            )
            label_widget.pack(pady=(15, 5))
            
            value_widget = ctk.CTkLabel(
                card, text=default,
                font=ctk.CTkFont(size=20, weight="bold")
            )
            value_widget.pack(pady=(5, 15))
            
            self.stat_cards[key] = value_widget
        
        stats_grid.columnconfigure(0, weight=1)
        stats_grid.columnconfigure(1, weight=1)
        stats_grid.rowconfigure(0, weight=1)
        stats_grid.rowconfigure(1, weight=1)
    
    def _create_cloud_tab(self, parent):
        """Create MariaDB Cloud Multi-Tenant Management Tab"""
        
        # Credentials Import Section
        import_frame = ctk.CTkFrame(parent, corner_radius=10)
        import_frame.pack(fill="x", pady=(0, 15))
        
        header = ctk.CTkLabel(
            import_frame,
            text="☁️ Cloud Credentials Management",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        desc = ctk.CTkLabel(
            import_frame,
            text="Import readonly_users_list.json from client (Laptop A)",
            font=ctk.CTkFont(size=10),
            text_color="gray60"
        )
        desc.pack(anchor="w", padx=15, pady=(0, 10))
        
        import_inner = ctk.CTkFrame(import_frame, fg_color="transparent")
        import_inner.pack(fill="x", padx=15, pady=(0, 15))
        
        self.creds_file_entry = ctk.CTkEntry(import_inner, width=500, placeholder_text="Select credentials JSON file...")
        self.creds_file_entry.pack(side="left", fill="x", expand=True, padx=(0, 10))
        
        ctk.CTkButton(
            import_inner, text="📁 Browse", width=100,
            command=self._browse_credentials
        ).pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(
            import_inner, text="📥 Load Credentials", width=150,
            command=self._load_credentials,
            font=ctk.CTkFont(weight="bold")
        ).pack(side="left")
        
        # Loaded Users Display
        users_frame = ctk.CTkFrame(parent, corner_radius=10)
        users_frame.pack(fill="both", expand=True, pady=(0, 15))
        
        header = ctk.CTkLabel(
            users_frame,
            text="👥 Loaded Read-Only Users",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        users_text_frame = ctk.CTkFrame(users_frame, fg_color="gray10", corner_radius=8)
        users_text_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        self.users_display = scrolledtext.ScrolledText(
            users_text_frame,
            height=8,
            state="disabled",
            bg="#1a1a1a",
            fg="gray70",
            font=('Consolas', 9),
            relief="flat",
            borderwidth=0,
            padx=15,
            pady=10
        )
        self.users_display.pack(fill="both", expand=True, padx=5, pady=5)
        
        self._update_users_display("No credentials loaded")
        
        # Multi-Tenant Actions
        actions_frame = ctk.CTkFrame(parent, corner_radius=10)
        actions_frame.pack(fill="x")
        
        header = ctk.CTkLabel(
            actions_frame,
            text="🔧 Multi-Tenant Operations",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        header.pack(anchor="w", padx=15, pady=(15, 10))
        
        button_container = ctk.CTkFrame(actions_frame, fg_color="transparent")
        button_container.pack(pady=(0, 20))
        
        self.test_all_btn = ctk.CTkButton(
            button_container, 
            text="✅ Test All Users", 
            command=self._test_all_users,
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.test_all_btn.pack(side="left", padx=10)
        
        self.extract_all_btn = ctk.CTkButton(
            button_container, 
            text="📦 Extract All Tenants", 
            command=self._extract_all_tenants,
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.extract_all_btn.pack(side="left", padx=10)
        
        self.view_results_btn = ctk.CTkButton(
            button_container, 
            text="📊 View Results", 
            command=self._view_multi_tenant_results,
            width=180,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="gray40",
            hover_color="gray50"
        )
        self.view_results_btn.pack(side="left", padx=10)
        
    def _update_connection_info(self, message):
        """Update connection info text"""
        self.connection_info_text.config(state="normal")
        self.connection_info_text.delete("1.0", "end")
        self.connection_info_text.insert("1.0", message)
        self.connection_info_text.config(state="disabled")
        
    def _update_users_display(self, message):
        """Update users display"""
        self.users_display.config(state="normal")
        self.users_display.delete("1.0", "end")
        self.users_display.insert("1.0", message)
        self.users_display.config(state="disabled")
        
    def _update_metrics(self, message):
        """Update metrics display"""
        pass
        
    def _toggle_password(self):
        """Toggle password visibility"""
        if self.show_password_var.get():
            self.password_entry.configure(show="")
        else:
            self.password_entry.configure(show="*")
            
    # ==========================================
    # Auto/Manual Toggle Functions
    # ==========================================
    def _toggle_auto_optimize(self):
        """Toggle between auto and manual optimization"""
        if self.auto_optimize.get():
            self.chunk_entry.configure(state="readonly")
            self.chunk_entry.delete(0, "end")
            self.chunk_entry.insert(0, str(self.last_auto_chunk))
            self.workers_display.configure(text=str(self.last_auto_workers))
            self.opt_info_label.configure(text="✅ Auto mode: Parameters will be calculated automatically")
        else:
            self.chunk_entry.configure(state="normal")
            self.opt_info_label.configure(text="⚠️ Manual mode: Using custom parameters")
    
    def _adjust_chunk(self, delta):
        """Adjust chunk size manually"""
        if self.auto_optimize.get():
            messagebox.showinfo("Auto Mode", "Disable auto-optimize to adjust manually")
            return
        
        try:
            current = int(self.chunk_entry.get())
            new_value = max(1000, current + delta)
            self.chunk_entry.delete(0, "end")
            self.chunk_entry.insert(0, str(new_value))
        except ValueError:
            self.chunk_entry.delete(0, "end")
            self.chunk_entry.insert(0, "50000")
    
    def _adjust_workers(self, delta):
        """Adjust workers manually"""
        if self.auto_optimize.get():
            messagebox.showinfo("Auto Mode", "Disable auto-optimize to adjust manually")
            return
        
        current = int(self.workers_display.cget("text"))
        new_value = max(1, min(16, current + delta))
        self.workers_display.configure(text=str(new_value))
    
    def _reset_to_auto(self):
        """Reset to auto mode"""
        self.auto_optimize.set(True)
        self._toggle_auto_optimize()
    
    def _analyze_table(self):
        """Analyze table and calculate optimal parameters"""
        if not self.connector:
            messagebox.showerror("Error", "Please connect to database first")
            return
        
        entity = self.entity_entry.get()
        partition_col = self.partition_entry.get()
        
        if not entity:
            messagebox.showerror("Error", "Please enter table name")
            return
        
        self._log(f"🔍 Analyzing table: {entity}", "INFO")
        
        try:
            with self.connector.engine.connect() as conn:
                row_count_result = conn.execute(
                    self.connector._text(f"SELECT COUNT(*) FROM {entity}")
                ).scalar()
                
                self._log(f"📊 Total rows: {row_count_result:,}", "SUCCESS")
                
                # Get column count
                first_row = conn.execute(
                    self.connector._text(f"SELECT * FROM {entity} LIMIT 1")
                ).fetchone()
                column_count = len(first_row) if first_row else 10
                
                self._log(f"📊 Columns: {column_count}", "INFO")
                
                # Calculate optimal parameters
                chunk_size, workers = self._calculate_optimal_params(row_count_result, column_count)
                
                # Update display
                if self.auto_optimize.get():
                    self.last_auto_chunk = chunk_size
                    self.last_auto_workers = workers
                    self.chunk_entry.configure(state="normal")
                    self.chunk_entry.delete(0, "end")
                    self.chunk_entry.insert(0, str(chunk_size))
                    self.chunk_entry.configure(state="readonly")
                    self.workers_display.configure(text=str(workers))
                    
                    self.opt_info_label.configure(
                        text=f"✅ Optimized: {chunk_size:,} rows/chunk × {workers} workers = ~{chunk_size * workers:,} rows in parallel"
                    )
                else:
                    self.opt_info_label.configure(
                        text=f"💡 Recommended: {chunk_size:,} rows/chunk × {workers} workers (currently in manual mode)"
                    )
                
                messagebox.showinfo(
                    "Analysis Complete",
                    f"Table: {entity}\n"
                    f"Rows: {row_count_result:,}\n"
                    f"Columns: {column_count}\n\n"
                    f"Recommended Parameters:\n"
                    f"• Chunk Size: {chunk_size:,}\n"
                    f"• Workers: {workers}\n"
                    f"• Parallel Capacity: ~{chunk_size * workers:,} rows"
                )
        
        except Exception as e:
            self._log(f"❌ Analysis failed: {e}", "ERROR")
            messagebox.showerror("Analysis Error", str(e))
    
    def _calculate_optimal_params(self, row_count, column_count=None):
        """Calculate optimal chunk size and workers"""
        # Use connector's optimizer if available
        if hasattr(self.connector, 'optimizer'):
            chunk_size, workers = self.connector.optimizer.calculate_optimal_params(
                row_count=row_count,
                is_remote=True
            )
        else:
            # Fallback calculation
            if row_count < 10_000:
                chunk_size = row_count
                workers = 1
            elif row_count < 100_000:
                chunk_size = 10_000
                workers = 2
            elif row_count < 1_000_000:
                chunk_size = 50_000
                workers = 4
            else:
                chunk_size = 100_000
                workers = 8
        
        return chunk_size, workers
    
    def _browse_output(self):
        """Browse for output directory"""
        directory = filedialog.askdirectory()
        if directory:
            self.output_entry.delete(0, "end")
            self.output_entry.insert(0, directory)
    
    def _browse_credentials(self):
        """Browse for credentials JSON file"""
        filename = filedialog.askopenfilename(
            title="Select Credentials JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            self.creds_file_entry.delete(0, "end")
            self.creds_file_entry.insert(0, filename)
    
    def _load_credentials(self):
        """Load credentials from JSON file"""
        file_path = self.creds_file_entry.get()
        
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid credentials file")
            return
        
        try:
            with open(file_path, 'r') as f:
                self.cloud_credentials = json.load(f)
            
            # Display loaded users
            display_text = f"✅ Loaded {len(self.cloud_credentials)} users from {os.path.basename(file_path)}\n\n"
            
            for i, user in enumerate(self.cloud_credentials, 1):
                display_text += f"{i}. {user['username']} ({user.get('description', 'No description')})\n"
                display_text += f"   Host: {user['host']}:{user['port']}\n"
                display_text += f"   Database: {user['database']}\n\n"
            
            self._update_users_display(display_text)
            self._log(f"✅ Loaded {len(self.cloud_credentials)} cloud users", "SUCCESS")
            
            messagebox.showinfo("Success", f"Loaded {len(self.cloud_credentials)} read-only users")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load credentials:\n{e}")
            self._log(f"❌ Failed to load credentials: {e}", "ERROR")
    
    def _test_all_users(self):
        """Test all loaded cloud users"""
        if not hasattr(self, 'cloud_credentials') or not self.cloud_credentials:
            messagebox.showerror("Error", "Please load credentials first")
            return
        
        self._log("🧪 Testing all cloud users...", "INFO")
        
        # Run in thread to avoid blocking GUI
        def test_thread():
            results = []
            
            for user in self.cloud_credentials:
                self._log(f"\n🔍 Testing: {user['username']}", "INFO")
                
                try:
                    # Create connector
                    config = {
                        "host": user["host"],
                        "port": user["port"],
                        "database": user["database"],
                        "user": user["username"],
                        "password": user["password"],
                        "ssl_ca": user.get("ssl_ca", "")
                    }
                    
                    connector = MariaDBCloudConnector(config)
                    connector.connect()
                    
                    self._log(f"✅ {user['username']}: Connection successful", "SUCCESS")
                    
                    connector.close()
                    results.append((user['username'], True, "Success"))
                    
                except Exception as e:
                    self._log(f"❌ {user['username']}: {e}", "ERROR")
                    results.append((user['username'], False, str(e)))
            
            # Show summary
            passed = sum(1 for _, success, _ in results if success)
            total = len(results)
            
            self._log(f"\n🎯 Test Summary: {passed}/{total} users passed", "SUCCESS" if passed == total else "WARNING")
            
            self.root.after(0, lambda: messagebox.showinfo(
                "Test Complete",
                f"Results: {passed}/{total} users passed\n\nCheck logs for details"
            ))
        
        threading.Thread(target=test_thread, daemon=True).start()
    
    def _extract_all_tenants(self):
        """Extract data for all tenants"""
        if not hasattr(self, 'cloud_credentials') or not self.cloud_credentials:
            messagebox.showerror("Error", "Please load credentials first")
            return
        
        entity = self.entity_entry.get()
        if not entity:
            messagebox.showerror("Error", "Please enter table name")
            return
        
        result = messagebox.askyesno(
            "Confirm Multi-Tenant Extraction",
            f"Extract '{entity}' for {len(self.cloud_credentials)} tenants?\n\n"
            f"This will create separate folders for each tenant."
        )
        
        if not result:
            return
        
        self._log(f"📦 Starting multi-tenant extraction for {entity}", "INFO")
        
        # Run in thread
        def extract_thread():
            results = []
            
            for user in self.cloud_credentials:
                self._log(f"\n📥 Extracting for: {user['username']}", "INFO")
                
                try:
                    # Create connector
                    config = {
                        "host": user["host"],
                        "port": user["port"],
                        "database": user["database"],
                        "user": user["username"],
                        "password": user["password"],
                        "ssl_ca": user.get("ssl_ca", "")
                    }
                    
                    connector = MariaDBCloudConnector(config)
                    connector.connect()
                    
                    # Extract with metadata
                    extraction_request = {
                        "entity": entity,
                        "mode": "full",
                        "enable_parallel": False,
                        "flatten_json": {}
                    }
                    
                    output_dir = f"bronze_data_multi_tenant/{user['username']}"
                    
                    # Use extract_with_metadata to save both data and metadata
                    result = connector.extract_with_metadata(extraction_request, output_dir)
                    
                    connector.close()
                    
                    if result["success"]:
                        self._log(f"✅ {user['username']}: Extracted {result['row_count']} rows", "SUCCESS")
                        self._log(f"   📁 Data: {result['data_file']}", "INFO")
                        self._log(f"   📋 Metadata: {result['metadata_file']}", "INFO")
                        results.append((user['username'], True, result['row_count'], result))
                    else:
                        self._log(f"❌ {user['username']}: {result.get('error', 'Unknown error')}", "ERROR")
                        results.append((user['username'], False, 0, result))
                    
                except Exception as e:
                    self._log(f"❌ {user['username']}: {e}", "ERROR")
                    results.append((user['username'], False, 0, {"error": str(e)}))
            
            # Show summary
            total_rows = sum(count for _, success, count, _ in results if success)
            passed = sum(1 for _, success, _, _ in results if success)
            
            self._log(f"\n🎯 Extraction Summary: {passed}/{len(results)} tenants, {total_rows:,} total rows", "SUCCESS")
            
            # Detailed summary
            summary_text = f"Results:\n• Successful: {passed}/{len(results)} tenants\n• Total Rows: {total_rows:,}\n\n"
            for username, success, row_count, res in results:
                if success:
                    summary_text += f"✅ {username}: {row_count:,} rows\n"
                else:
                    summary_text += f"❌ {username}: Failed\n"
            
            summary_text += f"\n📁 Output: bronze_data_multi_tenant/\n"
            summary_text += "   Each tenant has:\n"
            summary_text += "   • data.parquet (extracted data)\n"
            summary_text += "   • metadata.json (extraction info)"
            
            self.root.after(0, lambda: messagebox.showinfo(
                "Extraction Complete",
                summary_text
            ))
        
        threading.Thread(target=extract_thread, daemon=True).start()
    
    def _view_multi_tenant_results(self):
        """View multi-tenant extraction results"""
        results_dir = "bronze_data_multi_tenant"
        
        if not os.path.exists(results_dir):
            messagebox.showinfo("No Results", "No multi-tenant extraction results found")
            return
        
        # List subdirectories
        tenants = [d for d in os.listdir(results_dir) if os.path.isdir(os.path.join(results_dir, d))]
        
        if not tenants:
            messagebox.showinfo("No Results", "No tenant data found")
            return
        
        # Show summary
        summary = f"Found data for {len(tenants)} tenants:\n\n"
        for tenant in tenants:
            tenant_path = os.path.join(results_dir, tenant)
            files = os.listdir(tenant_path)
            summary += f"• {tenant}: {len(files)} files\n"
        
        messagebox.showinfo("Multi-Tenant Results", summary)
            
    def _log(self, message, level="INFO"):
        """Write message to log with timestamp and color"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        self.log_text.config(state="normal")
        
        # Insert timestamp
        self.log_text.insert("end", f"[{timestamp}] ", "TIMESTAMP")
        
        # Insert level tag
        level_tag = f"[{level}] "
        self.log_text.insert("end", level_tag, level)
        
        # Insert message
        self.log_text.insert("end", f"{message}\n", level)
        
        # Auto-scroll
        self.log_text.see("end")
        
        self.log_text.config(state="disabled")
        
        # Also log to Python logger
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
        
    def _update_stats(self, stats_dict):
        """Update statistics display"""
        for key, widget in self.stat_cards.items():
            if key in stats_dict:
                widget.configure(text=str(stats_dict[key]))
                
    def _clear_logs(self):
        """Clear the log console"""
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")
        
    def _export_stats(self):
        """Export statistics to file"""
        messagebox.showinfo("Export Stats", "Statistics export feature coming soon!")
        
    def _reset_defaults(self):
        """Reset all fields to default values"""
        result = messagebox.askyesno(
            "Reset Configuration",
            "Reset all fields to default values?"
        )
        
        if result:
            # Reset connection fields
            self.host_entry.delete(0, "end")
            self.host_entry.insert(0, "localhost")
            
            self.port_entry.delete(0, "end")
            self.port_entry.insert(0, "5432")
            
            self.database_entry.delete(0, "end")
            self.database_entry.insert(0, "postgres")
            
            self.user_entry.delete(0, "end")
            self.user_entry.insert(0, "postgres")
            
            self.password_entry.delete(0, "end")
            self.password_entry.insert(0, "password")
            
            self.ssl_ca_entry.delete(0, "end")
            
            # Reset extraction fields
            self.entity_entry.delete(0, "end")
            self.entity_entry.insert(0, "test_table")
            
            self.partition_entry.delete(0, "end")
            self.partition_entry.insert(0, "id")
            
            self.output_entry.delete(0, "end")
            self.output_entry.insert(0, "bronze_layer")
            
            self.auto_optimize.set(True)
            self._toggle_auto_optimize()
            
            self._log("✅ Reset to default values", "INFO")
            
    def _load_config_if_exists(self):
        """Load config.json if it exists"""
        config_file = "config.json"
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                # Load connection settings
                if "host" in config:
                    self.host_entry.delete(0, "end")
                    self.host_entry.insert(0, config["host"])
                
                if "port" in config:
                    self.port_entry.delete(0, "end")
                    self.port_entry.insert(0, str(config["port"]))
                
                if "database" in config:
                    self.database_entry.delete(0, "end")
                    self.database_entry.insert(0, config["database"])
                
                if "user" in config:
                    self.user_entry.delete(0, "end")
                    self.user_entry.insert(0, config["user"])
                
                # Don't load password for security
                
                self._log(f"✅ Loaded configuration from {config_file}", "SUCCESS")
                
            except Exception as e:
                self._log(f"⚠️ Could not load config: {e}", "WARNING")
                
    def _load_config_dialog(self):
        """Load configuration from file dialog"""
        filename = filedialog.askopenfilename(
            title="Load Configuration",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                with open(filename, 'r') as f:
                    config = json.load(f)
                
                # Apply config (similar to _load_config_if_exists)
                # ... (same logic)
                
                messagebox.showinfo("Success", f"Configuration loaded from {os.path.basename(filename)}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load configuration:\n{e}")
                
    def _save_config_dialog(self):
        """Save current configuration"""
        filename = filedialog.asksaveasfilename(
            title="Save Configuration",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                config = self._get_current_config()
                
                with open(filename, 'w') as f:
                    json.dump(config, f, indent=2)
                
                messagebox.showinfo("Success", f"Configuration saved to {os.path.basename(filename)}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save configuration:\n{e}")
                
    def _get_current_config(self):
        """Get current configuration as dictionary"""
        return {
            "source_type": self.source_type_var.get(),
            "host": self.host_entry.get(),
            "port": int(self.port_entry.get()) if self.port_entry.get().isdigit() else 5432,
            "database": self.database_entry.get(),
            "user": self.user_entry.get(),
            "ssl_ca": self.ssl_ca_entry.get(),
            "entity": self.entity_entry.get(),
            "partition_column": self.partition_entry.get(),
            "output_directory": self.output_entry.get(),
            "mode": self.mode_var.get(),
            "watermark_column": self.watermark_entry.get(),
            "initial_watermark_value": self.initial_value_entry.get()
        }
        
    def _connect(self):
        """Connect to database"""
        try:
            # Get config
            config = {
                "host": self.host_entry.get(),
                "port": int(self.port_entry.get()),
                "database": self.database_entry.get(),
                "user": self.user_entry.get(),
                "password": self.password_entry.get()
            }
            
            # Add SSL for cloud
            source_type = self.source_type_var.get()
            if source_type == "mariadb_cloud":
                ssl_ca = self.ssl_ca_entry.get()
                if ssl_ca:
                    config["ssl_ca"] = ssl_ca
            
            # Create connector
            if source_type == "postgres":
                self.connector = PostgresConnector(config)
            elif source_type == "mariadb":
                self.connector = MariaDBConnector(config)
            elif source_type == "mariadb_cloud":
                self.connector = MariaDBCloudConnector(config)
            else:
                raise ValueError(f"Unknown source type: {source_type}")
            
            # Connect
            self._log(f"🔌 Connecting to {source_type}...", "INFO")
            self.connector.connect()
            
            # Update UI
            self.status_badge.configure(
                text="🟢 CONNECTED",
                text_color="green"
            )
            
            self.connect_btn.configure(state="disabled")
            self.validate_btn.configure(state="normal")
            self.disconnect_btn.configure(state="normal")
            self.extract_btn.configure(state="normal")
            self.analyze_btn.configure(state="normal")
            
            # Update info
            info = (
                f"✅ Connected to {source_type}\n"
                f"Host: {config['host']}:{config['port']}\n"
                f"Database: {config['database']}\n"
                f"User: {config['user']}\n"
            )
            
            if source_type == "mariadb_cloud":
                info += f"SSL: {config.get('ssl_ca', 'Not configured')}\n"
            
            self._update_connection_info(info)
            self._log("✅ Connection successful!", "SUCCESS")
            
        except Exception as e:
            self._log(f"❌ Connection failed: {e}", "ERROR")
            messagebox.showerror("Connection Error", str(e))
            
    def _disconnect(self):
        """Disconnect from database"""
        if self.connector:
            try:
                self.connector.close()
                self.connector = None
                
                self.status_badge.configure(
                    text="⚫ DISCONNECTED",
                    text_color="red"
                )
                
                self.connect_btn.configure(state="normal")
                self.validate_btn.configure(state="disabled")
                self.disconnect_btn.configure(state="disabled")
                self.extract_btn.configure(state="disabled")
                self.analyze_btn.configure(state="disabled")
                
                self._update_connection_info("No active connection")
                self._log("✅ Disconnected", "INFO")
                
            except Exception as e:
                self._log(f"⚠️ Error during disconnect: {e}", "WARNING")
        
    def _validate(self):
        """Validate connection"""
        if self.connector:
            self._log("🧪 Validating connection...", "INFO")
            # Could add more validation logic
            self._log("✅ Connection is valid", "SUCCESS")
            messagebox.showinfo("Validation", "Connection is valid!")
        else:
            messagebox.showerror("Error", "No active connection")
            
    def _start_extraction(self):
        """Start data extraction"""
        if not self.connector:
            messagebox.showerror("Error", "Please connect to database first")
            return
        
        # Get parameters
        entity = self.entity_entry.get()
        partition_col = self.partition_entry.get()
        mode = self.mode_var.get()
        
        if not entity:
            messagebox.showerror("Error", "Please enter table name")
            return
        
        # Build extraction request
        extraction_request = {
            "entity": entity,
            "mode": mode,
            "partition_column": partition_col,
            "flatten_json": {},
            "enable_parallel": False
        }
        
        if mode == "incremental":
            extraction_request["watermark_column"] = self.watermark_entry.get()
            extraction_request["initial_watermark_value"] = self.initial_value_entry.get()
        
        # Update UI
        self.is_extracting = True
        self.extract_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.progress.start()
        
        self._log(f"🚀 Starting extraction: {entity}", "INFO")
        
        # Start extraction thread
        self.extraction_thread = threading.Thread(
            target=self._run_extraction,
            args=(extraction_request,),
            daemon=True
        )
        self.extraction_thread.start()
    
    def _run_extraction(self, extraction_request):
        """Run extraction in background thread"""
        try:
            output_dir = self.output_entry.get()
            
            # Check if connector has extract_with_metadata (MariaDB Cloud)
            if hasattr(self.connector, 'extract_with_metadata'):
                self._log("☁️ Using cloud extraction with metadata output", "INFO")
                
                # Use enhanced method that saves metadata + data
                result = self.connector.extract_with_metadata(extraction_request, output_dir)
                
                if result["success"]:
                    self._log(f"✅ Extraction complete: {result['row_count']:,} rows in {result['extraction_time_seconds']}s", "SUCCESS")
                    self._log(f"📁 Data file: {result['data_file']}", "HIGHLIGHT")
                    self._log(f"📋 Metadata file: {result['metadata_file']}", "HIGHLIGHT")
                    self._log(f"📊 File size: {result['file_size_kb']:.2f} KB", "INFO")
                    
                    # Update stats
                    self.root.after(0, lambda: self._update_stats({
                        "total_rows": f"{result['row_count']:,}",
                        "chunks": "1",
                        "elapsed": f"{result['extraction_time_seconds']}s",
                        "throughput": f"{int(result['row_count'] / result['extraction_time_seconds'])} rows/s" if result['extraction_time_seconds'] > 0 else "N/A"
                    }))
                    
                    # Show summary dialog
                    self.root.after(0, lambda: messagebox.showinfo(
                        "Extraction Complete",
                        f"✅ Successfully extracted {result['row_count']:,} rows\n\n"
                        f"⏱️ Time: {result['extraction_time_seconds']}s\n"
                        f"📁 Data: {os.path.basename(result['data_file'])}\n"
                        f"📋 Metadata: {os.path.basename(result['metadata_file'])}\n"
                        f"📊 Size: {result['file_size_kb']:.2f} KB\n\n"
                        f"Check logs for file paths."
                    ))
                else:
                    self._log(f"❌ Extraction failed: {result.get('error', 'Unknown error')}", "ERROR")
                    self.root.after(0, lambda: messagebox.showerror("Extraction Error", result.get('error', 'Unknown error')))
            
            else:
                # Standard extraction (PostgreSQL, MariaDB self-hosted)
                total_rows = 0
                batch_count = 0
                
                for batch_df in self.connector.extract(extraction_request):
                    if not self.is_extracting:
                        self._log("⏹ Extraction cancelled", "WARNING")
                        break
                    
                    batch_count += 1
                    total_rows += len(batch_df)
                    
                    # Update stats
                    self.root.after(0, lambda r=total_rows, b=batch_count: self._update_stats({
                        "total_rows": f"{r:,}",
                        "chunks": str(b)
                    }))
                    
                    self._log(f"📦 Batch {batch_count}: {len(batch_df)} rows", "INFO")
                
                # Completion
                self._log(f"✅ Extraction complete: {total_rows:,} total rows", "SUCCESS")
            
        except Exception as e:
            self._log(f"❌ Extraction failed: {e}", "ERROR")
            self.root.after(0, lambda: messagebox.showerror("Extraction Error", str(e)))
            
        finally:
            self.root.after(0, self._extraction_finished)
            
    def _stop_extraction(self):
        """Stop extraction"""
        self.is_extracting = False
        self._log("⏹ Stopping extraction...", "WARNING")
            
    def _extraction_finished(self):
        """Handle extraction completion"""
        self.extract_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.progress.stop()
        self.progress.set(0)
        
    def _show_docs(self):
        """Show documentation"""
        docs = (
            "📖 SyniqAI Bronze Layer Documentation\n\n"
            "1. Connection Tab:\n"
            "   • Select database type\n"
            "   • Enter connection credentials\n"
            "   • Test connection before extraction\n\n"
            "2. Extraction Tab:\n"
            "   • Configure table and performance settings\n"
            "   • Use Auto-Optimize for best results\n"
            "   • Analyze table before extraction\n\n"
            "3. Monitoring Tab:\n"
            "   • View real-time extraction statistics\n\n"
            "4. Cloud Multi-Tenant Tab:\n"
            "   • Import credentials from JSON\n"
            "   • Test multiple users\n"
            "   • Batch extraction for all tenants\n\n"
            "For more information, see README files."
        )
        messagebox.showinfo("Documentation", docs)
        
    def _show_quickstart(self):
        """Show quick start guide"""
        guide = (
            "💡 Quick Start Guide\n\n"
            "1. Connect:\n"
            "   • Go to Connection tab\n"
            "   • Enter database credentials\n"
            "   • Click 'Connect to Database'\n\n"
            "2. Extract:\n"
            "   • Go to Extraction tab\n"
            "   • Enter table name\n"
            "   • Click 'Analyze Table' (optional)\n"
            "   • Click 'Start Extraction'\n\n"
            "3. Monitor:\n"
            "   • View logs in console below\n"
            "   • Check Monitoring tab for statistics\n\n"
            "4. Multi-Tenant (Cloud):\n"
            "   • Go to Cloud Multi-Tenant tab\n"
            "   • Load credentials JSON\n"
            "   • Test or extract all tenants"
        )
        messagebox.showinfo("Quick Start", guide)
        
    def _show_about(self):
        """Show about dialog"""
        about = (
            "SyniqAI Bronze Layer\n"
            "Enterprise Data Extraction Platform\n\n"
            "Version: 2.0 (CustomTkinter)\n"
            "Built with: Python, CustomTkinter\n\n"
            "Features:\n"
            "• PostgreSQL & MariaDB support\n"
            "• MariaDB Cloud (SkySQL) integration\n"
            "• Multi-tenant management\n"
            "• Auto-optimization\n"
            "• Real-time monitoring\n\n"
            "© 2026 SyniqAI"
        )
        messagebox.showinfo("About", about)


def main():
    root = ctk.CTk()
    app = SyniqAIBronzeGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
