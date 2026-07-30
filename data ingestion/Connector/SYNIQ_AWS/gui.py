import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import json
import threading
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Import the ingestion modules
import ingest_structured
import ingest_unstructured
import s3

class DataPipelineGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SYNIQ AWS Data Ingestion Pipeline")
        self.root.geometry("1400x900")
        self.root.minsize(1200, 700)
        self.root.resizable(True, True)
        
        # State variables
        self.is_processing = False
        self.s3_client = None
        self.bucket_info = {}
        self.download_location = str(Path.home() / "Downloads" / "s3_downloads")
        self.selected_sync_items = []  # Store selected files/folders for sync
        
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
            'download_purple': '#9B59B6',
            'download_orange': '#F39C12',
            'sync_teal': '#16A085'
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
            background=self.colors['bg_tertiary'],
            foreground=self.colors['accent'],
            font=('Segoe UI', 16, 'bold')
        )
        
        style.configure('Status.TLabel',
            background=self.colors['bg_tertiary'],
            foreground=self.colors['text_secondary'],
            font=('Segoe UI', 10)
        )
        
        # Button styles
        style.configure('TButton',
            background=self.colors['accent'],
            foreground=self.colors['text_primary'],
            borderwidth=0,
            focuscolor='none',
            font=('Segoe UI', 10)
        )
        
        style.map('TButton',
            background=[('active', self.colors['accent_hover'])],
            foreground=[('active', 'white')]
        )
        
        # Entry styles
        style.configure('TEntry',
            fieldbackground=self.colors['bg_tertiary'],
            foreground=self.colors['text_primary'],
            bordercolor=self.colors['border'],
            lightcolor=self.colors['bg_tertiary'],
            darkcolor=self.colors['bg_tertiary'],
            insertcolor=self.colors['text_primary']
        )
        
        # Notebook (tabs) styles
        style.configure('TNotebook',
            background=self.colors['bg_secondary'],
            borderwidth=0,
            tabmargins=[2, 5, 2, 0]
        )
        
        style.configure('TNotebook.Tab',
            background=self.colors['bg_tertiary'],
            foreground=self.colors['text_secondary'],
            padding=[20, 10],
            borderwidth=0,
            font=('Segoe UI', 11)
        )
        
        style.map('TNotebook.Tab',
            background=[('selected', self.colors['bg_secondary'])],
            foreground=[('selected', self.colors['accent'])],
            expand=[('selected', [1, 1, 1, 0])]
        )
        
        # LabelFrame styles
        style.configure('TLabelframe',
            background=self.colors['bg_secondary'],
            foreground=self.colors['text_primary'],
            bordercolor=self.colors['border'],
            darkcolor=self.colors['bg_secondary'],
            lightcolor=self.colors['bg_secondary']
        )
        
        style.configure('TLabelframe.Label',
            background=self.colors['bg_secondary'],
            foreground=self.colors['accent'],
            font=('Segoe UI', 11, 'bold')
        )
        
        # Progressbar style
        style.configure('Horizontal.TProgressbar',
            background=self.colors['accent'],
            troughcolor=self.colors['bg_tertiary'],
            bordercolor=self.colors['border'],
            lightcolor=self.colors['accent'],
            darkcolor=self.colors['accent']
        )
        
    def _create_menu(self):
        """Create menu bar"""
        menubar = tk.Menu(self.root,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            bd=0
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
        file_menu.add_command(label="Load .env Config", command=self._load_env_dialog)
        file_menu.add_command(label="Set Download Location", command=self._set_download_location)
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
        tools_menu.add_command(label="Test AWS Connection", command=self._test_aws_connection)
        tools_menu.add_command(label="Browse S3 Bucket", command=self._browse_s3_bucket)
        tools_menu.add_separator()
        tools_menu.add_command(label="Open Output Folder", command=self._open_output_folder)
        tools_menu.add_command(label="Open Download Folder", command=self._open_download_folder)
        
        # Help Menu
        help_menu = tk.Menu(menubar, tearoff=0,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            activebackground=self.colors['accent'],
            activeforeground='white',
            font=('Segoe UI', 10)
        )
        menubar.add_cascade(label="  Help  ", menu=help_menu)
        help_menu.add_command(label="Documentation", command=self._show_docs)
        help_menu.add_command(label="Quick Start Guide", command=self._show_quickstart)
        help_menu.add_separator()
        help_menu.add_command(label="About", command=self._show_about)
        
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
            text="SYNIQ AWS Data Pipeline", 
            style='Title.TLabel'
        )
        title_label.grid(row=0, column=0, sticky=tk.W)
        
        subtitle_label = ttk.Label(header_frame,
            text="Data Ingestion & S3 Sync Manager v2.0.0",
            style='Status.TLabel'
        )
        subtitle_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        
        # Connection status and exit button on the right
        status_frame = ttk.Frame(header_frame, style='Card.TFrame')
        status_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E, padx=(20, 0))
        
        self.status_badge = tk.Label(
            status_frame,
            text="[DISCONNECTED]",
            font=('Segoe UI', 10, 'bold'),
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_secondary'],
            padx=15,
            pady=8,
            relief=tk.FLAT,
            borderwidth=0
        )
        self.status_badge.pack(side=tk.LEFT, padx=(0, 10))
        
        # Exit button next to status
        exit_btn = tk.Button(
            status_frame,
            text="EXIT",
            font=('Segoe UI', 9, 'bold'),
            bg='#E74C3C',
            fg='white',
            activebackground='#C0392B',
            activeforeground='white',
            bd=0,
            padx=15,
            pady=6,
            cursor="hand2",
            command=self.root.quit
        )
        exit_btn.pack(side=tk.LEFT)
        
        header_frame.columnconfigure(0, weight=1)
        
        # Notebook (tabbed interface)
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        
        # Instructions tab
        instructions_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        self._create_instructions_tab(instructions_tab)
        notebook.add(instructions_tab, text="  Instructions  ")
        
        # Configuration tab
        config_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        self._create_config_tab(config_tab)
        notebook.add(config_tab, text="  Configuration  ")
        
        # Local Data Operations tab
        local_operations_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        self._create_local_operations_tab(local_operations_tab)
        notebook.add(local_operations_tab, text="  Local Operations  ")
        
        # S3 Operations tab
        s3_operations_tab = ttk.Frame(notebook, style='TFrame', padding="20")
        self._create_s3_operations_tab(s3_operations_tab)
        notebook.add(s3_operations_tab, text="  S3 Operations  ")
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate', style='Horizontal.TProgressbar')
        self.progress.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Console/logs section
        console_frame = ttk.LabelFrame(main_frame, text="  Console Output  ", style='TLabelframe', padding="10")
        console_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.log_text = scrolledtext.ScrolledText(
            console_frame,
            height=15,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            font=('Consolas', 9),
            insertbackground=self.colors['text_primary'],
            selectbackground=self.colors['accent'],
            selectforeground='white',
            borderwidth=0,
            relief=tk.FLAT
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Configure text tags for colored output
        self.log_text.tag_config("SUCCESS", foreground=self.colors['success'])
        self.log_text.tag_config("ERROR", foreground=self.colors['error'])
        self.log_text.tag_config("WARNING", foreground=self.colors['warning'])
        self.log_text.tag_config("HIGHLIGHT", foreground=self.colors['accent'])
        self.log_text.tag_config("TIMESTAMP", foreground=self.colors['text_secondary'])
        
        # Configure grid weights
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        main_container.grid_rowconfigure(0, weight=1)
        main_container.grid_columnconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        
        # Log initialization
        self._log("SYNIQ AWS Data Pipeline GUI initialized")
        self._log(f"Download location: {self.download_location}")
    
    def _create_instructions_tab(self, parent):
        """Create instructions and how-to-use guide"""
        
        # Main container
        container = ttk.Frame(parent, style='TFrame')
        container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(
            container,
            text="SYNIQ AWS Data Pipeline - User Guide",
            style='Title.TLabel',
            foreground=self.colors['accent']
        )
        title_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 20))
        
        # Instructions text
        instructions_text = """
OVERVIEW
========
This application allows you to manage data transfers between local storage and AWS S3, as well as 
synchronize data between different S3 buckets.

WORKFLOW
========
1. INSTRUCTIONS (This Tab)
   - Read this guide to understand how to use the application
   
2. CONFIGURATION
   - Configure your AWS credentials and S3 bucket settings
   - Set up client S3 credentials if you need S3-to-S3 sync
   - Test your AWS connection
   
3. LOCAL OPERATIONS
   - Upload local files (CSV/Excel/Images/Videos/PDFs) to S3
   - Download files from S3 to your local machine
   - All operations use your Main AWS configuration
   
4. S3 OPERATIONS
   - Sync files from a client's S3 bucket to your S3 bucket
   - Requires Client S3 configuration with separate credentials
   - Only new files are copied (deduplication enabled)

GETTING STARTED
===============
Step 1: Go to "Configuration" tab
   - Enter your AWS Region (e.g., ap-southeast-1)
   - Enter your S3 Bucket name
   - Set Output Directory for local metadata
   - Click "Test AWS Connection" to verify

Step 2: For Local Operations
   - Go to "Local Operations" tab
   - Upload: Select structured (CSV/Excel) or unstructured (Images/Videos/PDFs) data
   - Download: Choose to download all files or just structured data
   - Set download location via File menu

Step 3: For S3-to-S3 Sync (Optional)
   - Go to "Configuration" tab, scroll to Client S3 section
   - Enter client's bucket name, prefix, and access keys
   - Go to "S3 Operations" tab
   - Click "Sync from Client S3 to Destination S3"

FEATURES
========
- Automatic deduplication (files won't be re-uploaded/re-downloaded)
- SHA-256 hashing for data integrity
- Performance metrics tracking
- Detailed logging in console output
- Progress tracking for all operations

AWS CREDENTIALS
===============
Main AWS Config uses credentials from:
   - AWS CLI configuration (~/.aws/credentials)
   - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
   - IAM role (if running on EC2/ECS/Lambda)

Client S3 Config requires:
   - Access Key ID and Secret Access Key from the client
   - These are stored locally in .env file
   - Only needed for S3-to-S3 sync operations

SUPPORT
=======
For issues or questions, check the console output for detailed error messages.
All operations are logged with timestamps for troubleshooting.
"""
        
        # Scrollable text widget for instructions
        text_widget = scrolledtext.ScrolledText(
            container,
            wrap=tk.WORD,
            bg=self.colors['bg_tertiary'],
            fg=self.colors['text_primary'],
            font=('Consolas', 10),
            borderwidth=0,
            relief=tk.FLAT,
            padx=15,
            pady=15
        )
        text_widget.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        text_widget.insert(1.0, instructions_text)
        text_widget.config(state=tk.DISABLED)
        
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        
    def _create_config_tab(self, parent):
        """Create AWS configuration panel with separate tabs"""
        
        # Create sub-notebook for configuration tabs
        config_notebook = ttk.Notebook(parent, style='TNotebook')
        config_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=10)
        
        # Tab 1: Main AWS Configuration
        aws_tab = ttk.Frame(config_notebook, style='TFrame')
        config_notebook.add(aws_tab, text='Main AWS Config')
        self._create_main_aws_tab(aws_tab)
        
        # Tab 2: Client S3 Configuration
        client_tab = ttk.Frame(config_notebook, style='TFrame')
        config_notebook.add(client_tab, text='Client S3 Config')
        self._create_client_s3_tab(client_tab)
        
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
    
    def _create_main_aws_tab(self, parent):
        """Create Main AWS Configuration tab with instructions and test button"""
        
        # Main container with scrollable area
        container = ttk.Frame(parent, style='TFrame')
        container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=20, pady=20)
        
        # === INSTRUCTIONS SECTION ===
        instructions_frame = ttk.LabelFrame(container, text="  How to Use Main AWS Configuration  ", 
            style='TLabelframe', padding="20")
        instructions_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        instructions_text = """PURPOSE:
This is your primary AWS account configuration for:
• Uploading local files to S3
• Downloading S3 files to local storage

WHAT EACH FIELD MEANS:
• AWS Region: The geographic region where your S3 bucket is located (e.g., ap-southeast-1, us-east-1)
• S3 Bucket: The name of your S3 bucket where files will be uploaded/downloaded
• Output Directory: Local folder name where downloaded files and processing results are saved

CREDENTIALS:
This configuration uses your default AWS credentials from:
1. AWS CLI configuration (~/.aws/credentials)
2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
3. IAM role (if running on EC2/ECS)

NO MANUAL KEYS NEEDED: Just configure AWS CLI once with 'aws configure' command."""
        
        instructions_label = ttk.Label(
            instructions_frame,
            text=instructions_text,
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.LEFT,
            font=('Segoe UI', 9)
        )
        instructions_label.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # === CONFIGURATION FIELDS ===
        config_frame = ttk.LabelFrame(container, text="  Configuration Fields  ", 
            style='TLabelframe', padding="25")
        config_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        # AWS Region
        ttk.Label(config_frame, text="AWS Region:", style='TLabel').grid(
            row=0, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.aws_region_entry = ttk.Entry(config_frame, width=50, style='TEntry')
        self.aws_region_entry.insert(0, "ap-southeast-1")
        self.aws_region_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # S3 Bucket
        ttk.Label(config_frame, text="S3 Bucket:", style='TLabel').grid(
            row=1, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        bucket_frame = ttk.Frame(config_frame, style='TFrame')
        bucket_frame.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=8)
        
        self.s3_bucket_entry = ttk.Entry(bucket_frame, width=40, style='TEntry')
        self.s3_bucket_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        browse_bucket_btn = tk.Button(
            bucket_frame,
            text="Browse Buckets",
            font=('Segoe UI', 9, 'bold'),
            bg=self.colors['accent'],
            fg='white',
            activebackground=self.colors['accent_hover'],
            activeforeground='white',
            bd=0,
            padx=15,
            pady=6,
            cursor="hand2",
            command=self._browse_buckets
        )
        browse_bucket_btn.pack(side=tk.LEFT)
        
        # Output Directory
        ttk.Label(config_frame, text="Output Directory:", style='TLabel').grid(
            row=2, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.output_dir_entry = ttk.Entry(config_frame, width=50, style='TEntry')
        self.output_dir_entry.insert(0, "output")
        self.output_dir_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=8)
        
        config_frame.columnconfigure(1, weight=1)
        
        # === TEST CONNECTION BUTTON (CENTERED) ===
        test_frame = ttk.Frame(container, style='TFrame')
        test_frame.grid(row=2, column=0, pady=(20, 0))
        
        self.test_aws_btn = tk.Button(
            test_frame,
            text="Test AWS Connection",
            font=('Segoe UI', 12, 'bold'),
            bg=self.colors['accent'],
            fg='white',
            activebackground=self.colors['accent_hover'],
            activeforeground='white',
            bd=0,
            padx=40,
            pady=15,
            cursor="hand2",
            command=self._test_aws_connection,
            width=30
        )
        self.test_aws_btn.pack()
        
        container.columnconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
    
    def _create_client_s3_tab(self, parent):
        """Create Client S3 Configuration tab with detailed instructions and test button"""
        
        # Main container
        container = ttk.Frame(parent, style='TFrame')
        container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=20, pady=20)
        
        # === INSTRUCTIONS SECTION ===
        instructions_frame = ttk.LabelFrame(container, text="  What is Client S3 Configuration?  ", 
            style='TLabelframe', padding="20")
        instructions_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        instructions_text = """IMPORTANT: This is ONLY for S3-to-S3 Bucket Synchronization!

PURPOSE:
This configuration is for syncing data between TWO DIFFERENT AWS accounts:
• Source Account: Client/Partner S3 bucket (configured here)
• Destination Account: Your main AWS S3 bucket (configured in Main AWS Config tab)

WHEN TO USE:
✓ When you need to copy files from a client's/partner's S3 bucket to your S3 bucket
✓ For cross-account S3 bucket synchronization
✓ When client provides their S3 access credentials

WHEN NOT TO USE:
✗ Regular file uploads from your computer (use Local Operations tab)
✗ Downloading files to your computer (use Local Operations tab)
✗ Working with your own S3 bucket only (use Main AWS Config)

WHAT EACH FIELD DOES:
• Client Bucket: The name of the client's S3 bucket you want to sync FROM
• Client Prefix: Optional folder path in client's bucket (e.g., 'exports/data/')
• Access Key ID: Client's AWS access key (provided by client)
• Secret Access Key: Client's AWS secret key (provided by client)

SECURITY NOTES:
⚠ These credentials access the CLIENT'S AWS account, not yours
⚠ Only request READ permissions from client
⚠ Store credentials securely - they are shown as masked (****) for security"""
        
        instructions_label = ttk.Label(
            instructions_frame,
            text=instructions_text,
            style='TLabel',
            foreground=self.colors['warning'],
            justify=tk.LEFT,
            font=('Segoe UI', 9)
        )
        instructions_label.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # === CONFIGURATION FIELDS ===
        config_frame = ttk.LabelFrame(container, text="  Client S3 Configuration Fields  ", 
            style='TLabelframe', padding="25")
        config_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        # Client Bucket
        ttk.Label(config_frame, text="Client Bucket:", style='TLabel').grid(
            row=0, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        client_bucket_frame = ttk.Frame(config_frame, style='TFrame')
        client_bucket_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=8)
        
        self.client_bucket_entry = ttk.Entry(client_bucket_frame, width=35, style='TEntry')
        self.client_bucket_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        browse_client_bucket_btn = tk.Button(
            client_bucket_frame,
            text="Browse",
            font=('Segoe UI', 9, 'bold'),
            bg=self.colors['sync_teal'],
            fg='white',
            activebackground='#138D75',
            activeforeground='white',
            bd=0,
            padx=15,
            pady=6,
            cursor="hand2",
            command=self._browse_client_buckets
        )
        browse_client_bucket_btn.pack(side=tk.LEFT)
        
        ttk.Label(config_frame, text="(Client's S3 bucket name)", 
            style='TLabel', foreground=self.colors['text_secondary'], 
            font=('Segoe UI', 8, 'italic')).grid(
            row=0, column=2, sticky=tk.W, pady=8, padx=(5, 0)
        )
        
        # Client Prefix
        ttk.Label(config_frame, text="Client Prefix:", style='TLabel').grid(
            row=1, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_prefix_entry = ttk.Entry(config_frame, width=50, style='TEntry')
        self.client_prefix_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=8)
        
        ttk.Label(config_frame, text="(Optional: e.g., 'exports/' or 'data/files/')", 
            style='TLabel', foreground=self.colors['text_secondary'], 
            font=('Segoe UI', 8, 'italic')).grid(
            row=1, column=2, sticky=tk.W, pady=8, padx=(5, 0)
        )
        
        # Client Access Key
        ttk.Label(config_frame, text="Access Key ID:", style='TLabel').grid(
            row=2, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_ak_entry = ttk.Entry(config_frame, width=50, style='TEntry', show="*")
        self.client_ak_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=8)
        
        ttk.Label(config_frame, text="(Provided by client)", 
            style='TLabel', foreground=self.colors['text_secondary'], 
            font=('Segoe UI', 8, 'italic')).grid(
            row=2, column=2, sticky=tk.W, pady=8, padx=(5, 0)
        )
        
        # Client Secret Key
        ttk.Label(config_frame, text="Secret Access Key:", style='TLabel').grid(
            row=3, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_sk_entry = ttk.Entry(config_frame, width=50, style='TEntry', show="*")
        self.client_sk_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=8)
        
        ttk.Label(config_frame, text="(Provided by client)", 
            style='TLabel', foreground=self.colors['text_secondary'], 
            font=('Segoe UI', 8, 'italic')).grid(
            row=3, column=2, sticky=tk.W, pady=8, padx=(5, 0)
        )
        
        config_frame.columnconfigure(1, weight=1)
        
        # === TEST CONNECTION BUTTON (CENTERED) ===
        test_frame = ttk.Frame(container, style='TFrame')
        test_frame.grid(row=2, column=0, pady=(20, 0))
        
        self.test_client_btn = tk.Button(
            test_frame,
            text="Test Client S3 Connection",
            font=('Segoe UI', 12, 'bold'),
            bg=self.colors['sync_teal'],
            fg='white',
            activebackground='#138D75',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=15,
            cursor="hand2",
            command=self._test_client_s3_connection,
            width=30
        )
        self.test_client_btn.pack()
        
        container.columnconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
    
    def _create_aws_config_section(self, parent):
        """Create main AWS configuration section with instructions"""
        
        # AWS Configuration Box (LEFT COLUMN)
        aws_section = ttk.LabelFrame(parent, text="  Main AWS Configuration (Local / S3)  ", 
            style='TLabelframe', padding="25")
        aws_section.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # Instructions
        instructions_text = """HOW TO USE: This is your primary AWS account for uploading local files to S3 or downloading S3 files to local.
Uses your default AWS credentials from: AWS CLI (~/.aws/credentials), Environment variables, or IAM role."""
        
        instructions_label = ttk.Label(
            aws_section,
            text=instructions_text,
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.LEFT,
            font=('Segoe UI', 9)
        )
        instructions_label.grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 15))
        
        # AWS Region
        ttk.Label(aws_section, text="AWS Region:", style='TLabel').grid(
            row=1, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.aws_region_entry = ttk.Entry(aws_section, width=50, style='TEntry')
        self.aws_region_entry.insert(0, "ap-southeast-1")
        self.aws_region_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # S3 Bucket
        ttk.Label(aws_section, text="S3 Bucket:", style='TLabel').grid(
            row=2, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.s3_bucket_entry = ttk.Entry(aws_section, width=50, style='TEntry')
        self.s3_bucket_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # Output Directory
        ttk.Label(aws_section, text="Output Directory:", style='TLabel').grid(
            row=3, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.output_dir_entry = ttk.Entry(aws_section, width=50, style='TEntry')
        self.output_dir_entry.insert(0, "output")
        self.output_dir_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=8)
        
        aws_section.columnconfigure(1, weight=1)
    
    def _create_client_config_section(self, parent):
        """Create client S3 configuration section with instructions"""
        
        # Client S3 Configuration Box (RIGHT COLUMN)
        client_section = ttk.LabelFrame(parent, text="  Client S3 Configuration (S3 to S3 Sync)  ", 
            style='TLabelframe', padding="25")
        client_section.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(10, 0))
        
        # Instructions
        instructions_text = """HOW TO USE: This is a SEPARATE AWS account (client/source) for S3-to-S3 bucket synchronization only.
IMPORTANT: Only needed for "S3 Bucket Sync" operations. For regular uploads/downloads, use Main AWS Config above."""
        
        instructions_label = ttk.Label(
            client_section,
            text=instructions_text,
            style='TLabel',
            foreground=self.colors['warning'],
            justify=tk.LEFT,
            font=('Segoe UI', 9, 'bold')
        )
        instructions_label.grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=(0, 15))
        
        # Client Bucket
        ttk.Label(client_section, text="Client Bucket:", style='TLabel').grid(
            row=1, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_bucket_entry = ttk.Entry(client_section, width=50, style='TEntry')
        self.client_bucket_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # Client Prefix
        ttk.Label(client_section, text="Client Prefix:", style='TLabel').grid(
            row=2, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_prefix_entry = ttk.Entry(client_section, width=50, style='TEntry')
        self.client_prefix_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=8)
        
        ttk.Label(client_section, text="(Optional: e.g., 'exports/')", 
            style='TLabel', foreground=self.colors['text_secondary'], 
            font=('Segoe UI', 8, 'italic')).grid(
            row=2, column=2, sticky=tk.W, pady=8, padx=(5, 0)
        )
        
        # Client Access Key
        ttk.Label(client_section, text="Access Key ID:", style='TLabel').grid(
            row=3, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_ak_entry = ttk.Entry(client_section, width=50, style='TEntry', show="*")
        self.client_ak_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=8)
        
        # Client Secret Key
        ttk.Label(client_section, text="Secret Access Key:", style='TLabel').grid(
            row=4, column=0, sticky=tk.W, pady=8, padx=(0, 10)
        )
        
        self.client_sk_entry = ttk.Entry(client_section, width=50, style='TEntry', show="*")
        self.client_sk_entry.grid(row=4, column=1, sticky=(tk.W, tk.E), pady=8)
        
        client_section.columnconfigure(1, weight=1)
        
    def _create_local_operations_tab(self, parent):
        """Create local data operations panel with separate upload and download sections"""
        
        # Main container
        container = ttk.Frame(parent, style='TFrame')
        container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=10)
        
        # ==== UPLOAD SECTION (TOP) ====
        upload_section = ttk.LabelFrame(container, text="  Upload: Local to S3  ", 
            style='TLabelframe', padding="25")
        upload_section.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        # Upload instructions
        upload_instructions = ttk.Label(
            upload_section,
            text="Upload local files to AWS S3 - Files will be processed with deduplication",
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.CENTER,
            font=('Segoe UI', 10)
        )
        upload_instructions.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        # Destination label
        dest_label = ttk.Label(
            upload_section,
            text="Destination: S3 Bucket (from config)",
            style='TLabel',
            foreground=self.colors['upload_blue'],
            font=('Segoe UI', 10, 'bold')
        )
        dest_label.grid(row=1, column=0, columnspan=2, pady=(0, 20))
        
        # Upload buttons container
        upload_buttons = ttk.Frame(upload_section, style='TFrame')
        upload_buttons.grid(row=2, column=0, columnspan=2)
        
        # Upload Structured Data Button
        self.upload_structured_btn = tk.Button(
            upload_buttons,
            text="Upload Structured Data\n(CSV/Excel)",
            font=('Segoe UI', 13, 'bold'),
            bg=self.colors['upload_blue'],
            fg='white',
            activebackground='#2980B9',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._ingest_structured,
            width=25,
            justify=tk.CENTER
        )
        self.upload_structured_btn.grid(row=0, column=0, padx=10, pady=5)
        
        # Upload Unstructured Data Button
        self.upload_unstructured_btn = tk.Button(
            upload_buttons,
            text="Upload Unstructured Data\n(Images/Videos/PDFs)",
            font=('Segoe UI', 13, 'bold'),
            bg=self.colors['upload_green'],
            fg='white',
            activebackground='#27AE60',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._ingest_unstructured,
            width=25,
            justify=tk.CENTER
        )
        self.upload_unstructured_btn.grid(row=0, column=1, padx=10, pady=5)
        
        upload_section.columnconfigure(0, weight=1)
        upload_section.columnconfigure(1, weight=1)
        
        # ==== DOWNLOAD SECTION (BOTTOM) ====
        download_section = ttk.LabelFrame(container, text="  Download: S3 to Local  ", 
            style='TLabelframe', padding="25")
        download_section.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(15, 0))
        
        # Download instructions
        download_instructions = ttk.Label(
            download_section,
            text="Download files from S3 to your local machine - Files will be saved to the download location",
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.CENTER,
            font=('Segoe UI', 10)
        )
        download_instructions.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        # Download location display
        self.download_loc_label = ttk.Label(
            download_section,
            text=f"Save to: {self.download_location}",
            style='TLabel',
            foreground=self.colors['download_purple'],
            font=('Segoe UI', 10, 'bold'),
            wraplength=700
        )
        self.download_loc_label.grid(row=1, column=0, columnspan=2, pady=(0, 20))
        
        # Download buttons container
        download_buttons = ttk.Frame(download_section, style='TFrame')
        download_buttons.grid(row=2, column=0, columnspan=2)
        
        # Download All Files Button
        self.download_all_btn = tk.Button(
            download_buttons,
            text="Download All Files\nfrom S3 Bucket",
            font=('Segoe UI', 13, 'bold'),
            bg=self.colors['download_purple'],
            fg='white',
            activebackground='#8E44AD',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._download_all_from_s3,
            width=25,
            justify=tk.CENTER
        )
        self.download_all_btn.grid(row=0, column=0, padx=10, pady=5)
        
        # Download Structured Data Button
        self.download_structured_btn = tk.Button(
            download_buttons,
            text="Download Structured Data\n(Parquet files)",
            font=('Segoe UI', 13, 'bold'),
            bg=self.colors['download_orange'],
            fg='white',
            activebackground='#E67E22',
            activeforeground='white',
            bd=0,
            padx=40,
            pady=20,
            cursor="hand2",
            command=self._download_structured_from_s3,
            width=25,
            justify=tk.CENTER
        )
        self.download_structured_btn.grid(row=0, column=1, padx=10, pady=5)
        
        download_section.columnconfigure(0, weight=1)
        download_section.columnconfigure(1, weight=1)
        
        # Configure grid weights
        container.columnconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        
    def _create_s3_operations_tab(self, parent):
        """Create S3 bucket to bucket sync operations panel"""
        
        # Main container
        container = ttk.Frame(parent, style='TFrame')
        container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=20, pady=20)
        
        # Instructions section
        instructions_frame = ttk.Frame(container, style='Card.TFrame', padding="20")
        instructions_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        
        title_label = ttk.Label(
            instructions_frame,
            text="S3 Bucket Synchronization",
            style='Header.TLabel',
            foreground=self.colors['sync_teal']
        )
        title_label.pack(anchor=tk.W, pady=(0, 10))
        
        instructions_text = """HOW THIS WORKS:
This operation syncs files from a CLIENT's S3 bucket to YOUR S3 bucket.

REQUIREMENTS:
1. Client S3 Configuration must be completed (in Configuration tab)
2. Client must provide: Bucket name, Access Key ID, and Secret Access Key
3. Your Main AWS Configuration must be set up

WHAT HAPPENS:
- Files are copied FROM client's S3 bucket TO your S3 bucket
- Only NEW files are copied (existing files are skipped)
- Deduplication is automatic based on file metadata
- A manifest file tracks all copied files

USE CASES:
- Receiving data from client/partner AWS accounts
- Migrating data between different AWS accounts
- Regular data synchronization from external sources

SECURITY:
- Client credentials are used only to READ from their bucket
- Your credentials are used to WRITE to your bucket
- All credentials are stored locally and encrypted"""
        
        instructions_label = ttk.Label(
            instructions_frame,
            text=instructions_text,
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.LEFT,
            font=('Consolas', 9)
        )
        instructions_label.pack(anchor=tk.W, fill=tk.X)
        
        # Sync operation section
        sync_frame = ttk.LabelFrame(container, text="  S3 to S3 Sync Operation  ", 
            style='TLabelframe', padding="30")
        sync_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # Info label
        info_label = ttk.Label(
            sync_frame,
            text="Sync files from Client S3 bucket to your destination S3\nOnly new files will be copied (deduplication enabled)",
            style='TLabel',
            foreground=self.colors['text_secondary'],
            justify=tk.CENTER,
            font=('Segoe UI', 10)
        )
        info_label.pack(pady=(0, 20))
        
        # Browse Client S3 Button
        browse_client_btn = tk.Button(
            sync_frame,
            text="Browse Client S3 Bucket",
            font=('Segoe UI', 12, 'bold'),
            bg='#5DADE2',
            fg='white',
            activebackground='#3498DB',
            activeforeground='white',
            bd=0,
            padx=30,
            pady=15,
            cursor="hand2",
            command=self._browse_client_s3_contents,
            width=25,
            justify=tk.CENTER
        )
        browse_client_btn.pack(pady=(0, 15))
        
        # S3 Sync Button
        self.s3_sync_btn = tk.Button(
            sync_frame,
            text="Sync from Client S3\nto Destination S3",
            font=('Segoe UI', 14, 'bold'),
            bg=self.colors['sync_teal'],
            fg='white',
            activebackground='#138D75',
            activeforeground='white',
            bd=0,
            padx=50,
            pady=25,
            cursor="hand2",
            command=self._sync_s3,
            width=35,
            justify=tk.CENTER
        )
        self.s3_sync_btn.pack()
        
        container.columnconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        
    def _log(self, message, level="INFO"):
        """Add styled message to console"""
        self.log_text.config(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Insert timestamp
        self.log_text.insert(tk.END, f"[{timestamp}] ", "TIMESTAMP")
        
        # Determine message type and color
        if "completed" in message.lower() or "success" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "SUCCESS")
        elif "warning" in message.lower() or "skipped" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "WARNING")
        elif "error" in message.lower() or "failed" in message.lower():
            self.log_text.insert(tk.END, f"{message}\n", "ERROR")
        else:
            self.log_text.insert(tk.END, f"{message}\n", level)
        
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
        # Also print to console
        print(f"[{timestamp}] {message}")
        
    def _clear_logs(self):
        """Clear console logs"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self._log("Console cleared")
        
    def _load_env_config(self):
        """Load environment configuration from .env"""
        if os.path.exists(".env"):
            try:
                load_dotenv()
                
                aws_region = os.getenv('AWS_REGION', 'ap-southeast-1')
                s3_bucket = os.getenv('S3_BUCKET', '')
                dest_bucket = os.getenv('DEST_BUCKET', '')
                output_dir = os.getenv('OUTPUT_DIR', 'output')
                client_bucket = os.getenv('CLIENT_BUCKET', '')
                client_prefix = os.getenv('CLIENT_PREFIX', '')
                client_ak = os.getenv('CLIENT_AWS_ACCESS_KEY_ID', '')
                client_sk = os.getenv('CLIENT_AWS_SECRET_ACCESS_KEY', '')
                
                self.aws_region_entry.delete(0, tk.END)
                self.aws_region_entry.insert(0, aws_region)
                
                self.s3_bucket_entry.delete(0, tk.END)
                self.s3_bucket_entry.insert(0, s3_bucket or dest_bucket)
                
                self.output_dir_entry.delete(0, tk.END)
                self.output_dir_entry.insert(0, output_dir)
                
                self.client_bucket_entry.delete(0, tk.END)
                self.client_bucket_entry.insert(0, client_bucket)
                
                self.client_prefix_entry.delete(0, tk.END)
                self.client_prefix_entry.insert(0, client_prefix)
                
                self.client_ak_entry.delete(0, tk.END)
                self.client_ak_entry.insert(0, client_ak)
                
                self.client_sk_entry.delete(0, tk.END)
                self.client_sk_entry.insert(0, client_sk)
                
                self._log("Loaded configuration from .env")
            except Exception as e:
                self._log(f"Could not load .env: {e}", "WARNING")
        
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
                self._log(f"Configuration loaded from {filepath}")
            except Exception as e:
                messagebox.showerror("Load Error", f"Failed to load config: {e}")
    
    def _set_download_location(self):
        """Set the download location for S3 files"""
        directory = filedialog.askdirectory(
            title="Select Download Location",
            initialdir=self.download_location
        )
        if directory:
            self.download_location = directory
            self.download_loc_label.config(text=f"Save to: {self.download_location}")
            self._log(f"Download location set to: {self.download_location}")
            
    def _test_aws_connection(self):
        """Test AWS S3 connection with detailed information"""
        try:
            self._log("Testing AWS S3 connection...")
            self.progress.start()
            self.status_badge.config(text="[TESTING...]", foreground=self.colors['warning'])
            
            region = self.aws_region_entry.get()
            bucket = self.s3_bucket_entry.get()
            
            if not bucket:
                raise Exception("S3 bucket name is required")
            
            # Test connection
            self.s3_client = boto3.client('s3', region_name=region)
            
            # Get bucket information
            try:
                # Check bucket exists and we have access
                self.s3_client.head_bucket(Bucket=bucket)
                
                # Get bucket location
                location = self.s3_client.get_bucket_location(Bucket=bucket)
                bucket_region = location['LocationConstraint'] or 'us-east-1'
                
                # Get bucket objects count (limited sample)
                response = self.s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1000)
                object_count = response.get('KeyCount', 0)
                has_more = response.get('IsTruncated', False)
                
                # Try to get bucket size (approximate from sample)
                total_size = 0
                if 'Contents' in response:
                    for obj in response['Contents']:
                        total_size += obj.get('Size', 0)
                
                # Calculate approximate total if truncated
                if has_more:
                    # Get total count using paginator
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucket)
                    total_count = sum(1 for _ in pages for _ in pages)
                else:
                    total_count = object_count
                
                # Store bucket info
                self.bucket_info = {
                    'name': bucket,
                    'region': bucket_region,
                    'object_count': total_count,
                    'sample_size': total_size,
                    'has_more': has_more
                }
                
                # Show success popup with details
                info_msg = f"""Great! Successfully connected to AWS S3!

Here's what I found in your bucket:
   • Bucket Name: {bucket}
   • Region: {bucket_region}
   • Files: {total_count:,} {'(approximate count)' if has_more else 'total'}
   • Sample Size: {total_size / 1024 / 1024:.2f} MB

Everything looks good:
   ✓ Your AWS Region: {region}
   ✓ Connection is active and working
   ✓ You have read/write permissions

You're all set to start uploading or downloading data!"""
                
                self.status_badge.config(text="[CONNECTED]", foreground=self.colors['success'])
                self._log("AWS S3 connection test successful")
                self._log(f"Bucket: {bucket} | Objects: {total_count:,} | Region: {bucket_region}")
                
                messagebox.showinfo("Connection Successful!", info_msg)
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    raise Exception(f"Bucket '{bucket}' does not exist")
                elif error_code == '403':
                    raise Exception(f"Access denied to bucket '{bucket}'. Check your AWS credentials.")
                else:
                    raise Exception(f"AWS Error: {e.response['Error']['Message']}")
                    
        except NoCredentialsError:
            error_msg = """Oops! I can't find your AWS credentials.

Let's fix this - you have a few options:

1. Run 'aws configure' in your terminal (easiest way!)
2. Set up environment variables:
   • AWS_ACCESS_KEY_ID
   • AWS_SECRET_ACCESS_KEY

3. Create a ~/.aws/credentials file

Need help? Check the Instructions tab for more details."""
            self.status_badge.config(text="[NO CREDENTIALS]", foreground=self.colors['error'])
            self._log("AWS credentials not found", "ERROR")
            messagebox.showerror("Can't Find Credentials", error_msg)
            
        except PartialCredentialsError:
            error_msg = """Almost there! Your AWS credentials are incomplete.

Make sure you have both of these set up:
   • AWS_ACCESS_KEY_ID
   • AWS_SECRET_ACCESS_KEY

Both are required for the connection to work."""
            self.status_badge.config(text="[INCOMPLETE CREDS]", foreground=self.colors['error'])
            self._log("Incomplete AWS credentials", "ERROR")
            messagebox.showerror("Incomplete Credentials", error_msg)
            
        except Exception as e:
            self.status_badge.config(text="[FAILED]", foreground=self.colors['error'])
            self._log(f"Connection test failed: {str(e)}", "ERROR")
            messagebox.showerror("Connection Failed", f"Couldn't connect to AWS S3:\n\n{str(e)}")
        finally:
            self.progress.stop()
    
    def _test_client_s3_connection(self):
        """Test Client S3 connection with provided credentials"""
        try:
            self._log("Testing Client S3 connection...")
            self.progress.start()
            self.status_badge.config(text="[TESTING CLIENT...]", foreground=self.colors['warning'])
            
            client_bucket = self.client_bucket_entry.get()
            client_ak = self.client_ak_entry.get()
            client_sk = self.client_sk_entry.get()
            
            if not client_bucket:
                raise Exception("Client bucket name is required")
            if not client_ak or not client_sk:
                raise Exception("Client access key and secret key are required")
            
            # Create client S3 connection with provided credentials
            client_s3 = boto3.client(
                's3',
                aws_access_key_id=client_ak,
                aws_secret_access_key=client_sk
            )
            
            # Test connection - check bucket exists and credentials work
            try:
                # Check bucket exists and we have access
                client_s3.head_bucket(Bucket=client_bucket)
                
                # Get bucket location
                location = client_s3.get_bucket_location(Bucket=client_bucket)
                bucket_region = location['LocationConstraint'] or 'us-east-1'
                
                # Get object count (limited sample)
                response = client_s3.list_objects_v2(Bucket=client_bucket, MaxKeys=1000)
                object_count = response.get('KeyCount', 0)
                has_more = response.get('IsTruncated', False)
                
                # Calculate approximate size
                total_size = 0
                if 'Contents' in response:
                    for obj in response['Contents']:
                        total_size += obj.get('Size', 0)
                
                # Success message
                info_msg = f"""Perfect! Client S3 connection is working!

What I found in the client bucket:
   • Bucket Name: {client_bucket}
   • Region: {bucket_region}
   • Files: {object_count:,} {'(sample count)' if has_more else 'total'}
   • Sample Size: {total_size / 1024 / 1024:.2f} MB

Everything checks out:
   ✓ Credentials are valid
   ✓ Access verified
   ✓ Ready for S3-to-S3 sync

You can now use the "S3 Bucket Sync" feature to copy data from this bucket!"""
                
                self.status_badge.config(text="[CLIENT CONNECTED]", foreground=self.colors['success'])
                self._log("Client S3 connection test successful")
                self._log(f"Client Bucket: {client_bucket} | Objects: {object_count:,} | Region: {bucket_region}")
                
                messagebox.showinfo("Client Connection Successful!", info_msg)
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404' or error_code == 'NoSuchBucket':
                    raise Exception(f"The bucket '{client_bucket}' doesn't exist.\n\nTip: Click 'Browse' button next to the bucket field to see available buckets!")
                elif error_code == '403' or error_code == 'AccessDenied':
                    # Try to list buckets instead to verify credentials work
                    try:
                        buckets_response = client_s3.list_buckets()
                        bucket_names = [b['Name'] for b in buckets_response['Buckets']]
                        
                        if client_bucket not in bucket_names:
                            available_list = "\n   • ".join(bucket_names[:10])
                            more_text = f"\n   ... and {len(bucket_names) - 10} more" if len(bucket_names) > 10 else ""
                            raise Exception(f"Bucket '{client_bucket}' not found in this account.\n\nAvailable buckets:\n   • {available_list}{more_text}\n\nTip: Click 'Browse' to select from the list!")
                        else:
                            raise Exception(f"Access denied to '{client_bucket}'.\n\nYour credentials work, but this bucket has restricted permissions.\n\nCheck:\n   • Bucket policy allows your credentials\n   • IAM permissions include s3:GetObject, s3:ListBucket\n   • Bucket is in the correct AWS account")
                    except ClientError:
                        raise Exception(f"Credentials can't access '{client_bucket}'.\n\nPossible issues:\n   • Wrong Access Key or Secret Key\n   • Bucket is in a different AWS account\n   • IAM permissions too restrictive\n\nTip: Click 'Browse' to test credentials and see available buckets!")
                elif error_code == 'InvalidAccessKeyId':
                    raise Exception("The Access Key ID is invalid.\n\nDouble-check:\n   • No extra spaces\n   • Correct key from AWS IAM\n   • Key is not deactivated")
                elif error_code == 'SignatureDoesNotMatch':
                    raise Exception("The Secret Access Key seems incorrect. Please check it again.")
                else:
                    raise Exception(f"AWS Error: {e.response['Error']['Message']}")
                    
        except Exception as e:
            self.status_badge.config(text="[CLIENT FAILED]", foreground=self.colors['error'])
            self._log(f"Client S3 connection test failed: {str(e)}", "ERROR")
            messagebox.showerror("Connection Failed", f"Couldn't connect to Client S3:\n\n{str(e)}")
        finally:
            self.progress.stop()
    
    def _browse_buckets(self):
        """Browse and select from available S3 buckets"""
        try:
            self._log("Fetching available S3 buckets...")
            self.progress.start()
            
            region = self.aws_region_entry.get()
            s3_client = boto3.client('s3', region_name=region)
            
            # List all buckets
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            
            if not buckets:
                messagebox.showinfo("No Buckets", "No S3 buckets found in your AWS account.")
                return
            
            # Create selection dialog
            dialog = tk.Toplevel(self.root)
            dialog.title("Select S3 Bucket")
            dialog.geometry("600x500")
            dialog.configure(bg=self.colors['bg_secondary'])
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Title
            title_label = tk.Label(
                dialog,
                text="Choose an S3 Bucket",
                font=('Segoe UI', 14, 'bold'),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_primary']
            )
            title_label.pack(pady=20)
            
            # Info label
            info_label = tk.Label(
                dialog,
                text=f"Found {len(buckets)} bucket(s) in your account:",
                font=('Segoe UI', 10),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_secondary']
            )
            info_label.pack(pady=(0, 10))
            
            # Listbox with scrollbar
            list_frame = ttk.Frame(dialog, style='TFrame')
            list_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
            
            scrollbar = ttk.Scrollbar(list_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            bucket_listbox = tk.Listbox(
                list_frame,
                font=('Segoe UI', 11),
                bg=self.colors['bg_tertiary'],
                fg=self.colors['text_primary'],
                selectbackground=self.colors['accent'],
                selectforeground='white',
                yscrollcommand=scrollbar.set,
                height=15
            )
            bucket_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.config(command=bucket_listbox.yview)
            
            # Add buckets to listbox
            for bucket in sorted(buckets):
                bucket_listbox.insert(tk.END, bucket)
            
            # Select button
            def on_select():
                selection = bucket_listbox.curselection()
                if selection:
                    selected_bucket = bucket_listbox.get(selection[0])
                    self.s3_bucket_entry.delete(0, tk.END)
                    self.s3_bucket_entry.insert(0, selected_bucket)
                    self._log(f"Selected bucket: {selected_bucket}")
                    dialog.destroy()
                else:
                    messagebox.showwarning("No Selection", "Please select a bucket from the list.")
            
            # Double-click to select
            bucket_listbox.bind('<Double-Button-1>', lambda e: on_select())
            
            # Buttons
            btn_frame = ttk.Frame(dialog, style='TFrame')
            btn_frame.pack(pady=(0, 20))
            
            select_btn = tk.Button(
                btn_frame,
                text="Select Bucket",
                font=('Segoe UI', 11, 'bold'),
                bg=self.colors['accent'],
                fg='white',
                activebackground=self.colors['accent_hover'],
                activeforeground='white',
                bd=0,
                padx=30,
                pady=12,
                cursor="hand2",
                command=on_select
            )
            select_btn.pack(side=tk.LEFT, padx=5)
            
            cancel_btn = tk.Button(
                btn_frame,
                text="Cancel",
                font=('Segoe UI', 11),
                bg='#95A5A6',
                fg='white',
                activebackground='#7F8C8D',
                activeforeground='white',
                bd=0,
                padx=30,
                pady=12,
                cursor="hand2",
                command=dialog.destroy
            )
            cancel_btn.pack(side=tk.LEFT, padx=5)
            
            self._log(f"Loaded {len(buckets)} buckets")
            
        except NoCredentialsError:
            messagebox.showerror("No Credentials", "AWS credentials not found. Please configure AWS CLI or set credentials first.")
            self._log("AWS credentials not found", "ERROR")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to fetch buckets:\n\n{str(e)}")
            self._log(f"Failed to fetch buckets: {str(e)}", "ERROR")
        finally:
            self.progress.stop()
    
    def _browse_client_buckets(self):
        """Browse and select from client's S3 buckets using their credentials"""
        try:
            client_ak = self.client_ak_entry.get()
            client_sk = self.client_sk_entry.get()
            
            if not client_ak or not client_sk:
                messagebox.showwarning("Missing Credentials", "Please enter the client's Access Key and Secret Key first.")
                return
            
            self._log("Fetching client S3 buckets...")
            self.progress.start()
            
            # Create client S3 connection
            client_s3 = boto3.client(
                's3',
                aws_access_key_id=client_ak,
                aws_secret_access_key=client_sk
            )
            
            # List all buckets
            response = client_s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            
            if not buckets:
                messagebox.showinfo("No Buckets", "No S3 buckets found in client's AWS account.")
                return
            
            # Create selection dialog
            dialog = tk.Toplevel(self.root)
            dialog.title("Select Client S3 Bucket")
            dialog.geometry("600x500")
            dialog.configure(bg=self.colors['bg_secondary'])
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Title
            title_label = tk.Label(
                dialog,
                text="Choose Client's S3 Bucket",
                font=('Segoe UI', 14, 'bold'),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_primary']
            )
            title_label.pack(pady=20)
            
            # Info label
            info_label = tk.Label(
                dialog,
                text=f"Found {len(buckets)} bucket(s) in client's account:",
                font=('Segoe UI', 10),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_secondary']
            )
            info_label.pack(pady=(0, 10))
            
            # Listbox with scrollbar
            list_frame = ttk.Frame(dialog, style='TFrame')
            list_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
            
            scrollbar = ttk.Scrollbar(list_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            bucket_listbox = tk.Listbox(
                list_frame,
                font=('Segoe UI', 11),
                bg=self.colors['bg_tertiary'],
                fg=self.colors['text_primary'],
                selectbackground=self.colors['sync_teal'],
                selectforeground='white',
                yscrollcommand=scrollbar.set,
                height=15
            )
            bucket_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.config(command=bucket_listbox.yview)
            
            # Add buckets to listbox
            for bucket in sorted(buckets):
                bucket_listbox.insert(tk.END, bucket)
            
            # Select button
            def on_select():
                selection = bucket_listbox.curselection()
                if selection:
                    selected_bucket = bucket_listbox.get(selection[0])
                    self.client_bucket_entry.delete(0, tk.END)
                    self.client_bucket_entry.insert(0, selected_bucket)
                    self._log(f"Selected client bucket: {selected_bucket}")
                    dialog.destroy()
                else:
                    messagebox.showwarning("No Selection", "Please select a bucket from the list.")
            
            # Double-click to select
            bucket_listbox.bind('<Double-Button-1>', lambda e: on_select())
            
            # Buttons
            btn_frame = ttk.Frame(dialog, style='TFrame')
            btn_frame.pack(pady=(0, 20))
            
            select_btn = tk.Button(
                btn_frame,
                text="Select Bucket",
                font=('Segoe UI', 11, 'bold'),
                bg=self.colors['sync_teal'],
                fg='white',
                activebackground='#138D75',
                activeforeground='white',
                bd=0,
                padx=30,
                pady=12,
                cursor="hand2",
                command=on_select
            )
            select_btn.pack(side=tk.LEFT, padx=5)
            
            cancel_btn = tk.Button(
                btn_frame,
                text="Cancel",
                font=('Segoe UI', 11),
                bg='#95A5A6',
                fg='white',
                activebackground='#7F8C8D',
                activeforeground='white',
                bd=0,
                padx=30,
                pady=12,
                cursor="hand2",
                command=dialog.destroy
            )
            cancel_btn.pack(side=tk.LEFT, padx=5)
            
            self._log(f"Loaded {len(buckets)} client buckets")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidAccessKeyId':
                messagebox.showerror("Invalid Credentials", "The Access Key ID is invalid. Please check the credentials.")
            elif error_code == 'SignatureDoesNotMatch':
                messagebox.showerror("Invalid Credentials", "The Secret Access Key is invalid. Please check the credentials.")
            elif error_code == 'AccessDenied':
                # Client credentials don't have permission to list all buckets - this is normal!
                messagebox.showinfo("Limited Permissions", 
                    "The client credentials don't have permission to list all buckets.\n\n" +
                    "This is actually good security! Clients should have minimal permissions.\n\n" +
                    "What to do:\n" +
                    "   1. Ask the client for their exact bucket name\n" +
                    "   2. Type the bucket name manually in the field\n" +
                    "   3. Click 'Test Client S3 Connection' to verify access\n\n" +
                    "The sync will work fine once you have the correct bucket name!")
                self._log("Client credentials have restricted permissions (cannot list all buckets)", "WARNING")
            else:
                messagebox.showerror("Error", f"AWS Error: {e.response['Error']['Message']}")
            self._log(f"Failed to fetch client buckets: {str(e)}", "ERROR")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to fetch client buckets:\n\n{str(e)}")
            self._log(f"Failed to fetch client buckets: {str(e)}", "ERROR")
        finally:
            self.progress.stop()
    
    def _browse_s3_bucket(self):
        """Browse S3 bucket contents in a popup window"""
        try:
            region = self.aws_region_entry.get()
            bucket = self.s3_bucket_entry.get()
            
            if not bucket:
                messagebox.showwarning("No Bucket", "Please enter an S3 bucket name first")
                return
            
            # Create S3 client if not exists
            if not self.s3_client:
                self.s3_client = boto3.client('s3', region_name=region)
            
            self._log(f"Browsing bucket: {bucket}")
            self.progress.start()
            
            # Create popup window
            browser_window = tk.Toplevel(self.root)
            browser_window.title(f"S3 Browser - {bucket}")
            browser_window.geometry("800x600")
            browser_window.configure(bg=self.colors['bg_primary'])
            
            # Header
            header_frame = tk.Frame(browser_window, bg=self.colors['bg_secondary'], pady=10)
            header_frame.pack(fill=tk.X)
            
            tk.Label(
                header_frame,
                text=f"S3 Bucket: {bucket}",
                font=('Segoe UI', 12, 'bold'),
                bg=self.colors['bg_secondary'],
                fg=self.colors['accent']
            ).pack()
            
            # Listbox with scrollbar
            list_frame = tk.Frame(browser_window, bg=self.colors['bg_primary'])
            list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            
            scrollbar = tk.Scrollbar(list_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            listbox = tk.Listbox(
                list_frame,
                yscrollcommand=scrollbar.set,
                bg=self.colors['bg_tertiary'],
                fg=self.colors['text_primary'],
                font=('Consolas', 9),
                selectbackground=self.colors['accent'],
                selectforeground='white',
                borderwidth=0,
                highlightthickness=0
            )
            listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.config(command=listbox.yview)
            
            # Status label
            status_label = tk.Label(
                browser_window,
                text="Loading...",
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_secondary'],
                font=('Segoe UI', 9),
                pady=10
            )
            status_label.pack(fill=tk.X)
            
            # Load objects in background
            def load_objects():
                try:
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucket, PaginationConfig={'MaxItems': 500})
                    
                    count = 0
                    total_size = 0
                    
                    for page in pages:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                size = obj['Size']
                                modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M')
                                
                                # Format size
                                if size < 1024:
                                    size_str = f"{size} B"
                                elif size < 1024 * 1024:
                                    size_str = f"{size/1024:.1f} KB"
                                else:
                                    size_str = f"{size/1024/1024:.1f} MB"
                                
                                item_text = f"{key:60s} | {size_str:>10s} | {modified}"
                                listbox.insert(tk.END, item_text)
                                
                                count += 1
                                total_size += size
                    
                    status_text = f"Total: {count} objects | Size: {total_size/1024/1024:.2f} MB"
                    status_label.config(text=status_text)
                    self._log(f"Loaded {count} objects from {bucket}")
                    
                except Exception as e:
                    status_label.config(text=f"Error: {str(e)}")
                    self._log(f"Failed to browse bucket: {str(e)}", "ERROR")
                finally:
                    self.progress.stop()
            
            # Run in thread
            threading.Thread(target=load_objects, daemon=True).start()
            
        except Exception as e:
            self.progress.stop()
            messagebox.showerror("Browse Error", f"Failed to browse S3 bucket:\n{str(e)}")
    
    def _open_output_folder(self):
        """Open the output folder in file explorer"""
        output_dir = self.output_dir_entry.get()
        if os.path.exists(output_dir):
            os.startfile(output_dir)
            self._log(f"Opened output folder: {output_dir}")
        else:
            messagebox.showwarning("Folder Not Found", f"Output folder does not exist: {output_dir}")
    
    def _open_download_folder(self):
        """Open the download folder in file explorer"""
        if os.path.exists(self.download_location):
            os.startfile(self.download_location)
            self._log(f"Opened download folder: {self.download_location}")
        else:
            # Create if doesn't exist
            os.makedirs(self.download_location, exist_ok=True)
            os.startfile(self.download_location)
            self._log(f"Created and opened download folder: {self.download_location}")
    
    def _disable_all_buttons(self):
        """Disable all action buttons during processing"""
        self.is_processing = True
        buttons = [
            self.test_aws_btn,
            self.upload_structured_btn,
            self.upload_unstructured_btn,
            self.download_all_btn,
            self.download_structured_btn,
            self.s3_sync_btn
        ]
        for btn in buttons:
            btn.config(state=tk.DISABLED)
            
    def _enable_all_buttons(self):
        """Enable all action buttons after processing"""
        self.is_processing = False
        buttons = [
            self.test_aws_btn,
            self.upload_structured_btn,
            self.upload_unstructured_btn,
            self.download_all_btn,
            self.download_structured_btn,
            self.s3_sync_btn
        ]
        for btn in buttons:
            btn.config(state=tk.NORMAL)
    
    def _ingest_structured(self):
        """Ingest structured data (CSV/Excel) to S3"""
        self._disable_all_buttons()
        self.status_badge.config(text="[PROCESSING...]", foreground=self.colors['upload_blue'])
        self._log("="*80)
        self._log("STRUCTURED DATA UPLOAD STARTED")
        self._log("="*80)
        self.progress.start()
        
        # Confirmation
        bucket = self.s3_bucket_entry.get()
        if not bucket:
            messagebox.showwarning("Missing Bucket", "Hey, you need to set up your S3 bucket first! Check the Configuration tab.")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        confirm = messagebox.askyesno(
            "Ready to Upload?",
            f"About to upload your structured data (CSV/Excel) to:\n\n{bucket}\n\nLooks good?"
        )
        
        if not confirm:
            self._log("Upload cancelled by user")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        def run_ingest():
            try:
                ingest_structured.main()
                self.root.after(0, lambda: self.status_badge.config(text="[SUCCESS]", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("Structured data upload completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Upload Complete!", "All done! Your structured data has been uploaded successfully."))
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda: self.status_badge.config(text="[FAILED]", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"Upload failed: {error_msg}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Upload Failed", f"Something went wrong:\n\n{error_msg}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_ingest, daemon=True).start()
        
    def _ingest_unstructured(self):
        """Ingest unstructured data (Images/Videos/PDFs) to S3"""
        self._disable_all_buttons()
        self.status_badge.config(text="[PROCESSING...]", foreground=self.colors['upload_green'])
        self._log("="*80)
        self._log("UNSTRUCTURED DATA UPLOAD STARTED")
        self._log("="*80)
        self.progress.start()
        
        # Confirmation
        bucket = self.s3_bucket_entry.get()
        if not bucket:
            messagebox.showwarning("Missing Bucket", "Hey, you need to set up your S3 bucket first! Check the Configuration tab.")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        confirm = messagebox.askyesno(
            "Ready to Upload?",
            f"About to upload your unstructured data (images/videos/PDFs) to:\n\n{bucket}\n\nShall we proceed?"
        )
        
        if not confirm:
            self._log("Upload cancelled by user")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        def run_ingest():
            try:
                ingest_unstructured.main()
                self.root.after(0, lambda: self.status_badge.config(text="[SUCCESS]", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("Unstructured data upload completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Upload Complete!", "Perfect! Your unstructured data has been uploaded successfully."))
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda: self.status_badge.config(text="[FAILED]", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"Upload failed: {error_msg}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Upload Failed", f"Something went wrong:\n\n{error_msg}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_ingest, daemon=True).start()
        
    def _download_all_from_s3(self):
        """Download all files from S3 bucket to local directory"""
        self._disable_all_buttons()
        self.status_badge.config(text="[DOWNLOADING...]", foreground=self.colors['download_purple'])
        self._log("="*80)
        self._log("S3 DOWNLOAD ALL STARTED")
        self._log("="*80)
        self.progress.start()
        
        region = self.aws_region_entry.get()
        bucket = self.s3_bucket_entry.get()
        
        if not bucket:
            messagebox.showwarning("Missing Bucket", "Hey, you need to set up your S3 bucket first! Check the Configuration tab.")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        # Confirmation
        confirm = messagebox.askyesno(
            "Ready to Download?",
            f"I'll download everything from:\n\n{bucket}\n\nSaving to: {self.download_location}\n\nSound good?"
        )
        
        if not confirm:
            self._log("Download cancelled by user")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        def run_download():
            try:
                # Create S3 client
                s3_client = boto3.client('s3', region_name=region)
                
                # Create download directory
                os.makedirs(self.download_location, exist_ok=True)
                
                # List and download all objects
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket)
                
                downloaded_count = 0
                total_size = 0
                
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            key = obj['Key']
                            size = obj['Size']
                            
                            # Skip if key ends with / (it's a folder marker)
                            if key.endswith('/'):
                                continue
                            
                            # Normalize path for Windows (replace forward slashes)
                            normalized_key = key.replace('/', os.sep)
                            local_path = os.path.join(self.download_location, normalized_key)
                            local_dir = os.path.dirname(local_path)
                            
                            # Create directory if it doesn't exist
                            if local_dir and not os.path.exists(local_dir):
                                os.makedirs(local_dir, exist_ok=True)
                            
                            # Download file
                            s3_client.download_file(bucket, key, local_path)
                            
                            self.root.after(0, lambda k=key: self._log(f"Downloaded: {k}"))
                            downloaded_count += 1
                            total_size += size
                
                success_msg = f"Download complete! Here's what I got:\n\n• {downloaded_count} files downloaded\n• Total size: {total_size / 1024 / 1024:.2f} MB\n\nEverything saved to:\n{self.download_location}"
                
                self.root.after(0, lambda: self.status_badge.config(text="[SUCCESS]", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log(f"Download completed: {downloaded_count} files"))
                self.root.after(0, lambda: messagebox.showinfo("Download Complete!", success_msg))
                
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda: self.status_badge.config(text="[FAILED]", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"Download failed: {error_msg}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Download Failed", f"Something went wrong:\n\n{error_msg}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_download, daemon=True).start()
    
    def _download_structured_from_s3(self):
        """Download only structured data (parquet files) from S3"""
        self._disable_all_buttons()
        self.status_badge.config(text="[DOWNLOADING...]", foreground=self.colors['download_orange'])
        self._log("="*80)
        self._log("S3 DOWNLOAD STRUCTURED DATA STARTED")
        self._log("="*80)
        self.progress.start()
        
        region = self.aws_region_entry.get()
        bucket = self.s3_bucket_entry.get()
        
        if not bucket:
            messagebox.showwarning("No Bucket", "Please configure S3 bucket first")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        # Confirmation
        confirm = messagebox.askyesno(
            "Confirm Download",
            f"Download structured data (Parquet files) from:\n\nS3 Bucket: {bucket}\nTo: {self.download_location}\n\nContinue?"
        )
        
        if not confirm:
            self._log("Download cancelled by user")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        def run_download():
            try:
                # Create S3 client
                s3_client = boto3.client('s3', region_name=region)
                
                # Create download directory
                structured_dir = os.path.join(self.download_location, "structured_data")
                os.makedirs(structured_dir, exist_ok=True)
                
                # List and download parquet files
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket)
                
                downloaded_count = 0
                total_size = 0
                
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            key = obj['Key']
                            size = obj['Size']
                            
                            # Only download parquet files
                            if key.endswith('.parquet'):
                                # Skip if key ends with / (folder marker)
                                if key.endswith('/'):
                                    continue
                                    
                                # Create safe filename
                                filename = os.path.basename(key.replace('/', '_'))
                                local_path = os.path.join(structured_dir, filename)
                                
                                # Download file
                                s3_client.download_file(bucket, key, local_path)
                                
                                self.root.after(0, lambda k=key: self._log(f"Downloaded: {k}"))
                                downloaded_count += 1
                                total_size += size
                
                if downloaded_count == 0:
                    self.root.after(0, lambda: messagebox.showinfo("No Files", "No parquet files found in the bucket"))
                else:
                    success_msg = f"Download completed!\n\n{downloaded_count} parquet files downloaded\nTotal size: {total_size / 1024 / 1024:.2f} MB\n\nSaved to: {structured_dir}"
                    self.root.after(0, lambda: messagebox.showinfo("Success", success_msg))
                
                self.root.after(0, lambda: self.status_badge.config(text="[SUCCESS]", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log(f"Download completed: {downloaded_count} files"))
                
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda: self.status_badge.config(text="[FAILED]", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"Download failed: {error_msg}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Download failed:\n{error_msg}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_download, daemon=True).start()
    
    def _browse_client_s3_contents(self):
        """Browse Client S3 bucket contents and select files/folders to sync"""
        try:
            client_ak = self.client_ak_entry.get()
            client_sk = self.client_sk_entry.get()
            client_bucket = self.client_bucket_entry.get()
            
            if not client_ak or not client_sk:
                messagebox.showwarning("Missing Credentials", "Please enter the client's Access Key and Secret Key in the Configuration tab first.")
                return
            
            if not client_bucket:
                messagebox.showwarning("Missing Bucket", "Please enter the client's S3 bucket name in the Configuration tab first.")
                return
            
            self._log(f"Fetching contents from client bucket: {client_bucket}")
            self.progress.start()
            
            # Create client S3 connection
            client_s3 = boto3.client(
                's3',
                aws_access_key_id=client_ak,
                aws_secret_access_key=client_sk
            )
            
            # List objects in the bucket
            objects = []
            paginator = client_s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=client_bucket):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    size = obj['Size']
                    modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                    objects.append({
                        'key': key,
                        'size': size,
                        'modified': modified,
                        'size_mb': size / (1024 * 1024)
                    })
            
            if not objects:
                messagebox.showinfo("Empty Bucket", f"No files found in bucket: {client_bucket}")
                return
            
            # Create selection dialog
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Select Files to Sync from {client_bucket}")
            dialog.geometry("900x650")
            dialog.configure(bg=self.colors['bg_secondary'])
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Title
            title_label = tk.Label(
                dialog,
                text=f"Choose Files/Folders to Sync",
                font=('Segoe UI', 14, 'bold'),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_primary']
            )
            title_label.pack(pady=20)
            
            # Info label
            info_label = tk.Label(
                dialog,
                text=f"Found {len(objects)} file(s) in {client_bucket}. Select items to sync:",
                font=('Segoe UI', 10),
                bg=self.colors['bg_secondary'],
                fg=self.colors['text_secondary']
            )
            info_label.pack(pady=(0, 10))
            
            # Create frame for tree and scrollbar
            tree_frame = ttk.Frame(dialog, style='TFrame')
            tree_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
            
            # Create Treeview with checkboxes
            tree_scroll = ttk.Scrollbar(tree_frame)
            tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
            
            tree = ttk.Treeview(
                tree_frame,
                columns=('Size', 'Modified'),
                yscrollcommand=tree_scroll.set,
                selectmode='extended',
                height=20
            )
            tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            tree_scroll.config(command=tree.yview)
            
            # Configure columns
            tree.heading('#0', text='File Path')
            tree.heading('Size', text='Size (MB)')
            tree.heading('Modified', text='Last Modified')
            
            tree.column('#0', width=500, minwidth=300)
            tree.column('Size', width=120, anchor='center')
            tree.column('Modified', width=180, anchor='center')
            
            # Track checked items
            checked_items = set()
            
            # Add checkbox symbol function
            def is_checked(item):
                return item in checked_items
            
            # Insert items into tree
            for obj in sorted(objects, key=lambda x: x['key']):
                checkbox = '☐'
                item_id = tree.insert('', 'end',
                    text=f'{checkbox} {obj["key"]}',
                    values=(f'{obj["size_mb"]:.2f}', obj['modified']),
                    tags=('unchecked',)
                )
                tree.item(item_id, tags=(obj['key'],))
            
            # Toggle checkbox on click
            def toggle_check(event):
                item = tree.identify('item', event.x, event.y)
                if item:
                    tags = tree.item(item, 'tags')
                    if tags:
                        key = tags[0]
                        text = tree.item(item, 'text')
                        
                        if item in checked_items:
                            checked_items.remove(item)
                            tree.item(item, text=text.replace('☑', '☐'))
                        else:
                            checked_items.add(item)
                            tree.item(item, text=text.replace('☐', '☑'))
            
            tree.bind('<Button-1>', toggle_check)
            
            # Selection buttons frame
            btn_frame = ttk.Frame(dialog, style='TFrame')
            btn_frame.pack(pady=(0, 20))
            
            # Select All button
            def select_all():
                checked_items.clear()
                for item in tree.get_children():
                    checked_items.add(item)
                    text = tree.item(item, 'text')
                    tree.item(item, text=text.replace('☐', '☑'))
            
            select_all_btn = tk.Button(
                btn_frame,
                text="Select All",
                font=('Segoe UI', 10),
                bg='#5DADE2',
                fg='white',
                activebackground='#3498DB',
                activeforeground='white',
                bd=0,
                padx=20,
                pady=8,
                cursor="hand2",
                command=select_all
            )
            select_all_btn.pack(side=tk.LEFT, padx=5)
            
            # Deselect All button
            def deselect_all():
                checked_items.clear()
                for item in tree.get_children():
                    text = tree.item(item, 'text')
                    tree.item(item, text=text.replace('☑', '☐'))
            
            deselect_all_btn = tk.Button(
                btn_frame,
                text="Deselect All",
                font=('Segoe UI', 10),
                bg='#95A5A6',
                fg='white',
                activebackground='#7F8C8D',
                activeforeground='white',
                bd=0,
                padx=20,
                pady=8,
                cursor="hand2",
                command=deselect_all
            )
            deselect_all_btn.pack(side=tk.LEFT, padx=5)
            
            # Confirm button
            def confirm_selection():
                if not checked_items:
                    messagebox.showwarning("No Selection", "Please select at least one file to sync.")
                    return
                
                # Get the keys of selected items
                selected_keys = []
                for item in checked_items:
                    tags = tree.item(item, 'tags')
                    if tags:
                        selected_keys.append(tags[0])
                
                self.selected_sync_items = selected_keys
                self._log(f"Selected {len(selected_keys)} item(s) for sync")
                messagebox.showinfo("Selection Complete", f"Selected {len(selected_keys)} item(s) for sync.\n\nClick 'Sync from Client S3 to Destination S3' to proceed.")
                dialog.destroy()
            
            confirm_btn = tk.Button(
                btn_frame,
                text=f"Confirm Selection",
                font=('Segoe UI', 11, 'bold'),
                bg=self.colors['sync_teal'],
                fg='white',
                activebackground='#138D75',
                activeforeground='white',
                bd=0,
                padx=30,
                pady=10,
                cursor="hand2",
                command=confirm_selection
            )
            confirm_btn.pack(side=tk.LEFT, padx=15)
            
            # Cancel button
            cancel_btn = tk.Button(
                btn_frame,
                text="Cancel",
                font=('Segoe UI', 10),
                bg='#E74C3C',
                fg='white',
                activebackground='#C0392B',
                activeforeground='white',
                bd=0,
                padx=20,
                pady=8,
                cursor="hand2",
                command=dialog.destroy
            )
            cancel_btn.pack(side=tk.LEFT, padx=5)
            
            self._log(f"Loaded {len(objects)} files from {client_bucket}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                messagebox.showerror("Bucket Not Found", f"The bucket '{client_bucket}' does not exist.")
            elif error_code == 'InvalidAccessKeyId':
                messagebox.showerror("Invalid Credentials", "The Access Key ID is invalid. Please check the credentials.")
            elif error_code == 'SignatureDoesNotMatch':
                messagebox.showerror("Invalid Credentials", "The Secret Access Key is invalid. Please check the credentials.")
            elif error_code == 'AccessDenied':
                messagebox.showerror("Access Denied", "You don't have permission to access this bucket. Check the credentials and bucket policy.")
            else:
                messagebox.showerror("Error", f"AWS Error: {e.response['Error']['Message']}")
            self._log(f"Failed to browse client bucket: {str(e)}", "ERROR")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to browse client bucket:\n\n{str(e)}")
            self._log(f"Failed to browse client bucket: {str(e)}", "ERROR")
        finally:
            self.progress.stop()
        
    def _sync_s3(self):
        """Sync from client S3 to destination S3"""
        self._disable_all_buttons()
        self.status_badge.config(text="[SYNCING...]", foreground=self.colors['sync_teal'])
        self._log("="*80)
        self._log("S3 BUCKET SYNC STARTED")
        self._log("="*80)
        self.progress.start()
        
        client_bucket = self.client_bucket_entry.get()
        dest_bucket = self.s3_bucket_entry.get()
        
        if not client_bucket or not dest_bucket:
            messagebox.showwarning("Missing Configuration", "Please configure both Client and Destination S3 buckets")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        # Check if user selected specific items
        if self.selected_sync_items:
            item_list = "\n  - ".join(self.selected_sync_items[:10])
            if len(self.selected_sync_items) > 10:
                item_list += f"\n  ... and {len(self.selected_sync_items) - 10} more"
            confirm_msg = f"Sync selected items ({len(self.selected_sync_items)} items):\n\n  - {item_list}\n\nFrom: {client_bucket}\nTo: {dest_bucket}\n\nContinue?"
        else:
            confirm_msg = f"Sync ALL files:\n\nFrom: {client_bucket}\nTo: {dest_bucket}\n\nOnly new files will be copied.\n\nContinue?"
        
        # Confirmation
        confirm = messagebox.askyesno(
            "Confirm S3 Sync",
            confirm_msg
        )
        
        if not confirm:
            self._log("S3 sync cancelled by user")
            self._enable_all_buttons()
            self.progress.stop()
            return
        
        def run_sync():
            try:
                # Call the main function from s3 module with selected items
                # Pass None if no selection (sync all), otherwise pass the selected keys
                selected_keys = self.selected_sync_items if self.selected_sync_items else None
                s3.main(selected_keys=selected_keys)
                
                # Clear selection after successful sync
                self.selected_sync_items = []
                
                self.root.after(0, lambda: self.status_badge.config(text="[SUCCESS]", foreground=self.colors['success']))
                self.root.after(0, lambda: self._log("S3 sync completed successfully"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "S3 sync completed!"))
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda: self.status_badge.config(text="[FAILED]", foreground=self.colors['error']))
                self.root.after(0, lambda: self._log(f"S3 sync failed: {error_msg}", "ERROR"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"S3 sync failed:\n{error_msg}"))
            finally:
                self.root.after(0, self.progress.stop)
                self.root.after(0, self._enable_all_buttons)
        
        threading.Thread(target=run_sync, daemon=True).start()
        
    def _show_docs(self):
        """Show documentation"""
        docs = """SYNIQ AWS DATA PIPELINE DOCUMENTATION

CONFIGURATION:
1. Configure AWS credentials and S3 bucket settings
2. Set download location for files from S3
3. Test AWS connection before operations

UPLOAD (LOCAL TO S3):
- Upload Structured Data: CSV/Excel files converted to Parquet
- Upload Unstructured Data: Images, Videos, PDFs with SHA-256 hashing
- All uploads include deduplication

DOWNLOAD (S3 TO LOCAL):
- Download All Files: Complete bucket download
- Download Structured Data: Only Parquet files
- Files saved to configured download location

S3 BUCKET SYNC:
- Sync from Client S3 to Destination S3
- Only new files copied (deduplication)
- Requires client bucket credentials

OUTPUT STRUCTURE:
- Structured: output/data/structured/tables/{table_name}/
- Unstructured: output/data/unstructured/run_date/run_id/
- Metadata: output/metadata/

FEATURES:
- SHA-256 deduplication
- Performance metrics tracking
- Detailed logging
- Progress tracking
"""
        messagebox.showinfo("Documentation", docs)
        
    def _show_quickstart(self):
        """Show quick start guide"""
        guide = """QUICK START GUIDE

STEP 1: CONFIGURE
- Go to Configuration tab
- Enter AWS Region and S3 Bucket
- (Optional) Configure Client S3 for sync
- Click "Test AWS Connection"

STEP 2: UPLOAD DATA
- Go to Data Operations tab
- Left column: Upload local files to S3
  * Upload Structured Data (CSV/Excel)
  * Upload Unstructured Data (Images/Videos/PDFs)

STEP 3: DOWNLOAD DATA
- Go to Data Operations tab
- Right column: Download from S3 to local
  * Download All Files
  * Download Structured Data only
- Set download location in File menu

STEP 4: S3 SYNC (OPTIONAL)
- Go to S3 Bucket Sync tab
- Sync between two S3 buckets
- Requires client bucket credentials

TIPS:
- All operations show progress in console
- Files are deduplicated automatically
- Check output folder after upload
- Check download folder after download
"""
        messagebox.showinfo("Quick Start Guide", guide)
        
    def _show_about(self):
        """Show about dialog"""
        about_text = """SYNIQ AWS Data Pipeline
Version 2.0.0

Professional data ingestion and sync solution for AWS S3.

Features:
- Bidirectional data transfer (Upload & Download)
- S3 bucket synchronization
- SHA-256 deduplication
- Performance tracking
- Parquet conversion for structured data

(c) 2026 SYNIQ
"""
        messagebox.showinfo("About", about_text)

def main():
    root = tk.Tk()
    app = DataPipelineGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
