import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import json
import threading
import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Import the upload and output modules
import upload_images
import upload_txt
import upload_video
import outputs_image
import outputs_txt
import outputs_video

class TSDMediaPipelineGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("TSD Media Pipeline - MongoDB Upload & Export Manager")
        self.root.geometry("1200x900")
        self.root.minsize(1000, 700)
        self.root.resizable(True, True)
        
        # State variables
        self.mongo_client = None
        self.is_processing = False
        
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
            'border': '#3e3e42',
            'upload_blue': '#3498DB',
            'upload_green': '#2ECC71',
            'upload_red': '#E74C3C',
            'export_purple': '#9B59B6',
            'export_orange': '#F39C12',
            'export_teal': '#16A085'
        }
        
        # Configure root window
        self.root.configure(bg=self.colors['bg_primary'])
        
        # Apply professional theme
        self._apply_professional_theme()
        self._create_menu()
        self._create_widgets()
        self._load_env_config()
        
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
        file_menu.add_command(label="Load Environment Config", command=self._load_env_dialog)
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
        tools_menu.add_command(label="Test MongoDB Connection", command=self._test_connection)
        
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
            text="🗄️ TSD Media Pipeline", 
            style='Title.TLabel'
        )
        title_label.grid(row=0, column=0, sticky=tk.W)
        
        subtitle_label = ttk.Label(header_frame,
            text="MongoDB Upload & Export Manager v2.2.0",
            style='Status.TLabel'
        )
        subtitle_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        
        # Connection status badge
        self.status_badge = ttk.Label(header_frame,
            text="⚫ READY",
            style='Status.TLabel',
            foreground=self.colors['text_secondary']
        )
        self.status_badge.grid(row=0, column=1, sticky=tk.E, padx=(0, 10))
        
        # Exit button
        self.exit_btn = tk.Button(
            header_frame,
            text="❌ Exit",
            font=('Segoe UI', 10, 'bold'),
            bg=self.colors['error'],
            fg='white',
            activebackground='#d62c1a',
            activeforeground='white',
            bd=0,
            padx=20,
            pady=8,
            cursor="hand2",
            command=self._exit_application
        )
        self.exit_btn.grid(row=0, column=2, sticky=tk.E, padx=(10, 0))
        
        header_frame.columnconfigure(0, weight=1)
        
        # Create notebook (tabs)
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        
        # Tab 1: Connection
        connection_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(connection_tab, text="  🔌 Connection  ")
        self._create_connection_tab(connection_tab)
        
        # Tab 2: Upload
        upload_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(upload_tab, text="  📤 Upload  ")
        self._create_upload_tab(upload_tab)
        
        # Tab 3: Export
        export_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        notebook.add(export_tab, text="  📥 Export  ")
        self._create_export_tab(export_tab)
        
        # Console Section
        console_frame = ttk.LabelFrame(main_frame, text="  Execution Console  ", 
            style='TLabelframe', padding="15")
        console_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        console_container = ttk.Frame(console_frame, style='Card.TFrame', padding="10")
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
        self.progress = ttk.Progressbar(console_frame, mode='indeterminate', style='TProgressbar')
        self.progress.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        console_container.columnconfigure(0, weight=1)
        console_container.rowconfigure(0, weight=1)
        
        # Initial log message
        self._log("🚀 TSD Media Pipeline GUI initialized")
        self._log("💡 Configure MongoDB connection in the Connection tab")
        
    def _create_connection_tab(self, parent):
        """Create MongoDB connection configuration panel"""
        
        # Connection Type Selector
        type_frame = ttk.Frame(parent, style='Card.TFrame', padding="15")
        type_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        ttk.Label(type_frame, text="Connection Type:", style='Header.TLabel').grid(
            row=0, column=0, sticky=tk.W, padx=(0, 15)
        )
        
        self.conn_type_var = tk.StringVar(value="atlas")
        
        ttk.Radiobutton(
            type_frame, 
            text="MongoDB Atlas (Cloud)", 
            variable=self.conn_type_var,
            value="atlas",
            style='TRadiobutton'
        ).grid(row=0, column=1, padx=10)
        
        ttk.Radiobutton(
            type_frame,
            text="On-Premises",
            variable=self.conn_type_var,
            value="onprem",
            style='TRadiobutton'
        ).grid(row=0, column=2, padx=10)
        
        # MongoDB Configuration Card
        config_card = ttk.LabelFrame(parent, text="  MongoDB Configuration  ", 
            style='TLabelframe', padding="20")
        config_card.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        # Connection String (Atlas)
        ttk.Label(config_card, text="Connection URI:", style='TLabel').grid(
            row=0, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.mongo_uri_entry = ttk.Entry(config_card, width=60, style='TEntry')
        self.mongo_uri_entry.insert(0, "mongodb+srv://user:pass@cluster.mongodb.net/")
        self.mongo_uri_entry.grid(row=0, column=1, columnspan=3, sticky=(tk.W, tk.E), pady=8)
        
        # Database Name
        ttk.Label(config_card, text="Database Name:", style='TLabel').grid(
            row=1, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.db_name_entry = ttk.Entry(config_card, width=30, style='TEntry')
        self.db_name_entry.insert(0, "media_db")
        self.db_name_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # Batch Size
        ttk.Label(config_card, text="Batch Size:", style='TLabel').grid(
            row=1, column=2, sticky=tk.W, pady=8, padx=(20, 10)
        )
        
        self.batch_size_entry = ttk.Entry(config_card, width=15, style='TEntry')
        self.batch_size_entry.insert(0, "1000")
        self.batch_size_entry.grid(row=1, column=3, sticky=tk.W, pady=8)
        
        config_card.columnconfigure(1, weight=1)
        
        # Test Connection Buttons
        test_button_frame = ttk.Frame(parent, style='TFrame')
        test_button_frame.grid(row=2, column=0, pady=20)
        
        self.test_connection_btn = tk.Button(
            test_button_frame,
            text="🔍 Test MongoDB Connection",
            font=('Segoe UI', 12, 'bold'),
            bg=self.colors['accent'],
            fg='white',
            activebackground=self.colors['accent_hover'],
            activeforeground='white',
            bd=0,
            padx=30,
            pady=15,
            cursor="hand2",
            command=self._test_connection,
            width=30
        )
        self.test_connection_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.test_local_connection_btn = tk.Button(
            test_button_frame,
            text="💻 Test Local MongoDB",
            font=('Segoe UI', 12, 'bold'),
            bg=self.colors['success'],
            fg='white',
            activebackground='#3d9970',
            activeforeground='white',
            bd=0,
            padx=30,
            pady=15,
            cursor="hand2",
            command=self._test_local_connection,
            width=25
        )
        self.test_local_connection_btn.pack(side=tk.LEFT)
        
        # Connection Info Panel
        info_card = ttk.LabelFrame(parent, text="  Connection Status  ",
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
        
        self._update_connection_info("No connection test performed yet.\nUse 'Tools > Test MongoDB Connection' to verify settings.")
        
        parent.columnconfigure(0, weight=1)
        
    def _create_upload_tab(self, parent):
        """Create upload operations panel"""
        
        # Info card
        info_card = ttk.Frame(parent, style='Card.TFrame', padding="15")
        info_card.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        info_label = ttk.Label(
            info_card,
            text="📤 Upload media files to MongoDB\nAll files will be processed with batch operations and duplicate detection",
            style='TLabel',
            foreground=self.colors['text_secondary']
        )
        info_label.pack()
        
        # Upload buttons container
        buttons_container = ttk.Frame(parent, style='TFrame')
        buttons_container.grid(row=1, column=0, pady=(0, 15))
        
        # Upload Images Button
        self.upload_images_btn = tk.Button(
            buttons_container,
            text="📷 Upload Images",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['upload_blue'],
            fg='white',
            activebackground=self.colors['accent_hover'],
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._upload_images,
            width=25
        )
        self.upload_images_btn.grid(row=0, column=0, padx=15, pady=10)
        
        # Upload Text Button
        self.upload_text_btn = tk.Button(
            buttons_container,
            text="📄 Upload Text Files",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['upload_green'],
            fg='white',
            activebackground='#27AE60',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._upload_text,
            width=25
        )
        self.upload_text_btn.grid(row=1, column=0, padx=15, pady=10)
        
        # Upload Videos Button
        self.upload_videos_btn = tk.Button(
            buttons_container,
            text="🎬 Upload Videos",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['upload_red'],
            fg='white',
            activebackground='#C0392B',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._upload_videos,
            width=25
        )
        self.upload_videos_btn.grid(row=2, column=0, padx=15, pady=10)
        
        parent.columnconfigure(0, weight=1)
        
    def _create_export_tab(self, parent):
        """Create export operations panel"""
        
        # Info card
        info_card = ttk.Frame(parent, style='Card.TFrame', padding="15")
        info_card.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        info_label = ttk.Label(
            info_card,
            text="📥 Export media files from MongoDB\nFiles will be saved to output/data_metadata/ directory with ZIP packaging",
            style='TLabel',
            foreground=self.colors['text_secondary']
        )
        info_label.pack()
        
        # Export buttons container
        buttons_container = ttk.Frame(parent, style='TFrame')
        buttons_container.grid(row=1, column=0, pady=(0, 15))
        
        # Export Images Button
        self.export_images_btn = tk.Button(
            buttons_container,
            text="📷 Export Images",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['export_purple'],
            fg='white',
            activebackground='#8E44AD',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._export_images,
            width=25
        )
        self.export_images_btn.grid(row=0, column=0, padx=15, pady=10)
        
        # Export Text Button
        self.export_text_btn = tk.Button(
            buttons_container,
            text="📄 Export Text Files",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['export_orange'],
            fg='white',
            activebackground='#E67E22',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._export_text,
            width=25
        )
        self.export_text_btn.grid(row=1, column=0, padx=15, pady=10)
        
        # Export Videos Button
        self.export_videos_btn = tk.Button(
            buttons_container,
            text="🎬 Export Videos",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['export_teal'],
            fg='white',
            activebackground='#138D75',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._export_videos,
            width=25
        )
        self.export_videos_btn.grid(row=2, column=0, padx=15, pady=10)
        
        parent.columnconfigure(0, weight=1)
        
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
        elif "⚡" in message or "🚀" in message or "📊" in message or "📤" in message or "📥" in message:
            self.log_text.insert(tk.END, f"{message}\n", "HIGHLIGHT")
        else:
            self.log_text.insert(tk.END, f"{message}\n", level)
        
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
    def _update_connection_info(self, message):
        """Update connection info display"""
        self.connection_info_text.config(state=tk.NORMAL)
        self.connection_info_text.delete(1.0, tk.END)
        self.connection_info_text.insert(tk.END, message)
        self.connection_info_text.config(state=tk.DISABLED)
        
    def _clear_logs(self):
        """Clear console logs"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self._log("Console cleared")
        
    def _load_env_config(self):
        """Load environment configuration from on_prem.env"""
        if os.path.exists("on_prem.env"):
            try:
                load_dotenv("on_prem.env")
                mongo_uri = os.getenv('MONGO_URI')
                db_name = os.getenv('MONGO_DB', 'media_db')
                
                if mongo_uri:
                    self.mongo_uri_entry.delete(0, tk.END)
                    self.mongo_uri_entry.insert(0, mongo_uri)
                    self.db_name_entry.delete(0, tk.END)
                    self.db_name_entry.insert(0, db_name)
                    self._log("✅ Loaded configuration from on_prem.env")
                else:
                    self._log("⚠️ MONGO_URI not found in on_prem.env", "WARNING")
            except Exception as e:
                self._log(f"⚠️ Could not load on_prem.env: {e}", "WARNING")
        
    def _load_env_dialog(self):
        """Load environment file via dialog"""
        filepath = filedialog.askopenfilename(
            title="Select Environment File",
            filetypes=[("Environment files", "*.env"), ("All files", "*.*")]
        )
        if filepath:
            try:
                load_dotenv(filepath)
                self._load_env_config()
                self._log(f"📂 Configuration loaded from {filepath}")
            except Exception as e:
                messagebox.showerror("Load Error", f"Failed to load config: {e}")
                
    def _test_connection(self):
        """Test MongoDB connection"""
        try:
            self._log("🔍 Testing MongoDB connection...")
            self.progress.start()
            self.status_badge.config(text="🔄 TESTING...", foreground=self.colors['warning'])
            
            uri = self.mongo_uri_entry.get()
            db_name = self.db_name_entry.get()
            
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            
            db = client[db_name]
            collections = db.list_collection_names()
            
            info = f"""✅ Connection Successful!

Database: {db_name}
Collections: {len(collections)}
URI: {uri[:50]}...

Collections found:
{', '.join(collections[:5])}
{f'... and {len(collections) - 5} more' if len(collections) > 5 else ''}"""
            
            self._update_connection_info(info)
            self.status_badge.config(text="🟢 CONNECTED", foreground=self.colors['success'])
            self._log("✅ MongoDB connection test successful")
            
            client.close()
            
        except Exception as e:
            self._update_connection_info(f"❌ Connection Failed\n\nError: {str(e)}")
            self.status_badge.config(text="🔴 FAILED", foreground=self.colors['error'])
            self._log(f"❌ Connection test failed: {str(e)}", "ERROR")
            messagebox.showerror("Connection Error", str(e))
        finally:
            self.progress.stop()
    
    def _test_local_connection(self):
        """Test local MongoDB connection on localhost:27017"""
        try:
            self._log("💻 Testing local MongoDB connection on localhost:27017...")
            self.progress.start()
            self.status_badge.config(text="🔄 TESTING...", foreground=self.colors['warning'])
            
            # Test local MongoDB connection
            local_uri = "mongodb://localhost:27017/"
            client = MongoClient(local_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            
            # Get database list
            databases = client.list_database_names()
            
            # Get stats for each database
            db_info = []
            for db_name in databases:
                if db_name not in ['admin', 'config', 'local']:
                    db = client[db_name]
                    collections = db.list_collection_names()
                    db_info.append(f"  • {db_name} ({len(collections)} collections)")
            
            info = f"""✅ Local MongoDB Connection Successful!

Server: localhost:27017
Databases: {len(databases)}

User Databases:
{chr(10).join(db_info) if db_info else '  (No user databases found)'}

System Databases:
  • admin, config, local"""
            
            self._update_connection_info(info)
            self.status_badge.config(text="🟢 CONNECTED", foreground=self.colors['success'])
            self._log("✅ Local MongoDB connection test successful")
            
            client.close()
            
        except Exception as e:
            error_msg = str(e)
            if "Connection refused" in error_msg or "No connection could be made" in error_msg:
                info = f"""❌ Connection Failed

Error: Cannot connect to local MongoDB

Possible reasons:
  1. MongoDB is not installed on this computer
  2. MongoDB service is not running
  3. MongoDB is running on a different port

To start MongoDB:
  • Windows: net start MongoDB
  • Or check MongoDB service in Services app"""
            else:
                info = f"❌ Connection Failed\n\nError: {error_msg}"
            
            self._update_connection_info(info)
            self.status_badge.config(text="🔴 FAILED", foreground=self.colors['error'])
            self._log(f"❌ Local connection test failed: {error_msg}", "ERROR")
            messagebox.showerror("Local Connection Error", f"Failed to connect to local MongoDB:\n\n{error_msg}")
        finally:
            self.progress.stop()
            
    def _disable_all_buttons(self):
        """Disable all operation buttons"""
        self.upload_images_btn.config(state=tk.DISABLED)
        self.upload_text_btn.config(state=tk.DISABLED)
        self.upload_videos_btn.config(state=tk.DISABLED)
        self.export_images_btn.config(state=tk.DISABLED)
        self.export_text_btn.config(state=tk.DISABLED)
        self.export_videos_btn.config(state=tk.DISABLED)
        
    def _enable_all_buttons(self):
        """Enable all operation buttons"""
        self.upload_images_btn.config(state=tk.NORMAL)
        self.upload_text_btn.config(state=tk.NORMAL)
        self.upload_videos_btn.config(state=tk.NORMAL)
        self.export_images_btn.config(state=tk.NORMAL)
        self.export_text_btn.config(state=tk.NORMAL)
        self.export_videos_btn.config(state=tk.NORMAL)
        
    def _upload_images(self):
        """Upload images to MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📤 UPLOADING...", foreground=self.colors['upload_blue'])
        self._log("="*80)
        self._log("📤 IMAGE UPLOAD STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_upload():
            try:
                upload_images.main()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Image upload completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Image upload completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Upload failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Upload failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_upload, daemon=True).start()
        
    def _upload_text(self):
        """Upload text files to MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📤 UPLOADING...", foreground=self.colors['upload_green'])
        self._log("="*80)
        self._log("📤 TEXT UPLOAD STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_upload():
            try:
                upload_txt.main()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Text upload completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Text upload completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Upload failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Upload failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_upload, daemon=True).start()
        
    def _upload_videos(self):
        """Upload videos to MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📤 UPLOADING...", foreground=self.colors['upload_red'])
        self._log("="*80)
        self._log("📤 VIDEO UPLOAD STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_upload():
            try:
                upload_video.main()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Video upload completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Video upload completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Upload failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Upload failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_upload, daemon=True).start()
        
    def _export_images(self):
        """Export images from MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📥 EXPORTING...", foreground=self.colors['export_purple'])
        self._log("="*80)
        self._log("📥 IMAGE EXPORT STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_export():
            try:
                outputs_image.main()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Image export completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Image export completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Export failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Export failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_export, daemon=True).start()
        
    def _export_text(self):
        """Export text files from MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📥 EXPORTING...", foreground=self.colors['export_orange'])
        self._log("="*80)
        self._log("📥 TEXT EXPORT STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_export():
            try:
                outputs_txt.main()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Text export completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Text export completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Export failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Export failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_export, daemon=True).start()
        
    def _export_videos(self):
        """Export videos from MongoDB"""
        self._disable_all_buttons()
        self.status_badge.config(text="📥 EXPORTING...", foreground=self.colors['export_teal'])
        self._log("="*80)
        self._log("📥 VIDEO EXPORT STARTED")
        self._log("="*80)
        self.progress.start()
        
        def run_export():
            try:
                outputs_video.generate_video_outputs()
                self.root.after(0, lambda: self.status_badge.config(text="✅ SUCCESS", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("✅ Video export completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Video export completed!"))
            except Exception as e:
                self.root.after(0, lambda: self.status_badge.config(text="❌ FAILED", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"❌ Export failed: {str(e)}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Export failed:\n{str(e)}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_export, daemon=True).start()
        
    def _show_docs(self):
        """Show documentation"""
        docs = """TSD MEDIA PIPELINE DOCUMENTATION

CONNECTION:
1. Select connection type (Atlas/On-Prem)
2. Enter MongoDB URI and database name
3. Use 'Tools > Test MongoDB Connection' to verify
4. Connection info stored in on_prem.env

UPLOAD:
1. Go to 'Upload' tab
2. Click desired media type button
3. Select files using file picker
4. System processes with batch operations
5. Automatic duplicate detection via SHA-256

EXPORT:
1. Go to 'Export' tab
2. Click desired media type button
3. Files exported to output/data_metadata/
4. ZIP archive + JSONL metadata created
5. Uses last successful run_id

BATCH OPERATIONS:
- Default: 1,000 files per batch
- Configurable in Connection tab
- Reduces network overhead 500x
- Uses bulk_write for performance

MONITORING:
- Real-time console logs
- Color-coded status messages
- Progress indicator
- All details logged to terminal"""
        messagebox.showinfo("Documentation", docs)
        
    def _show_quickstart(self):
        """Show quick start"""
        quick = """QUICK START GUIDE

1️⃣ CONNECTION
   • Configure MongoDB URI in Connection tab
   • Test connection using Tools menu
   • Verify on_prem.env exists

2️⃣ UPLOAD FILES
   • Go to Upload tab
   • Choose: Images, Text, or Videos
   • Select files to upload
   • Wait for completion message

3️⃣ EXPORT FILES
   • Go to Export tab
   • Choose: Images, Text, or Videos
   • Files saved to output/ directory
   • Check ZIP and JSONL files

✅ Done! Check console for detailed logs."""
        messagebox.showinfo("Quick Start", quick)
        
    def _show_about(self):
        """Show about"""
        about = """TSD Media Pipeline v2.2.0

MongoDB Upload & Export Manager

🎯 Features:
• Batch upload operations (1000 files/batch)
• Automatic duplicate detection (SHA-256)
• ZIP + JSONL export format
• Real-time progress monitoring
• Professional dark theme UI

🗄️ Supported Media:
• Images: JPG, PNG, TIFF, BMP, WEBP
• Text: TXT files
• Videos: MP4, AVI, MOV

📊 Performance:
• 22.4x faster than v1.0
• 26+ files/second throughput
• 500x fewer network calls
• Connection pooling (50 connections)

© 2026 TSD Media Pipeline Team"""
        messagebox.showinfo("About TSD Media Pipeline", about)
    
    def _exit_application(self):
        """Exit the application with confirmation"""
        if self.is_processing:
            response = messagebox.askyesno(
                "Exit Confirmation", 
                "A process is currently running.\nAre you sure you want to exit?",
                icon='warning'
            )
            if not response:
                return
        
        self._log("👋 Exiting TSD Media Pipeline...")
        self.root.quit()
        self.root.destroy()

def main():
    root = tk.Tk()
    app = TSDMediaPipelineGUI(root)
    
    print("\n" + "="*80)
    print("TSD MEDIA PIPELINE - GUI MODE v2.2.0")
    print("="*80)
    print("🚀 Professional MongoDB Upload & Export Manager")
    print("📊 All operations will be logged to this terminal")
    print("💡 Use the GUI to select and process files")
    print("="*80 + "\n")
    
    root.mainloop()

if __name__ == "__main__":
    main()
