"""
Data Tables - View and explore data
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data_loader import DataLoader

PROJECT_ROOT = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
loader = DataLoader(PROJECT_ROOT)


def show():
    """Display data tables page"""
    
    st.markdown("# Data Tables")
    st.markdown("### Explore and Analyze Gold Layer Tables")
    st.markdown("---")
    
    # Load available tables
    tables = loader.get_available_tables()
    
    if not tables:
        st.warning("WARNING: No data found. Please run Gold layer processing first.")
        st.code("python main.py gold process", language="bash")
        return
    
    # Table selector
    st.markdown("### Select a Table")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Source selection
        sources = list(set([t["source"] for t in tables]))
        selected_source = st.selectbox("Source System", sources)
    
    with col2:
        # Table selection
        source_tables = [t for t in tables if t["source"] == selected_source]
        table_options = {f"{t['table']}": t for t in source_tables}
        selected_table_name = st.selectbox("Table Name", list(table_options.keys()))
    
    if selected_table_name:
        selected_table = table_options[selected_table_name]
        
        st.markdown("---")
        
        # Load data
        df = loader.load_table_data(selected_source, selected_table_name)
        
        if df is not None:
            # Table info
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
                st.metric("Memory", f"{memory_mb:.2f} MB")
            with col4:
                st.metric("Last Modified", selected_table["last_modified"].split()[0])
            
            st.markdown("---")
            
            # Filters
            st.markdown("### Filters & Search")
            
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                # Column filter
                all_columns = ["All Columns"] + list(df.columns)
                selected_columns = st.multiselect(
                    "Select Columns to Display",
                    all_columns,
                    default=["All Columns"]
                )
            
            with col2:
                # Search
                search_term = st.text_input("🔎 Search in data", "")
            
            with col3:
                # Limit rows
                row_limit = st.selectbox("Show rows", [10, 25, 50, 100, 500], index=1)
            
            st.markdown("---")
            
            # Display data
            st.markdown("### Data Preview")
            
            # Apply filters
            display_df = df.copy()
            
            # Column selection
            if "All Columns" not in selected_columns and selected_columns:
                display_df = display_df[selected_columns]
            
            # Search filter
            if search_term:
                mask = display_df.astype(str).apply(
                    lambda row: row.str.contains(search_term, case=False, na=False).any(),
                    axis=1
                )
                display_df = display_df[mask]
            
            # Limit rows
            display_df = display_df.head(row_limit)
            
            # Show data
            st.dataframe(
                display_df,
                use_container_width=True,
                height=500
            )
            
            st.markdown("---")
            
            # Data Statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Column Types")
                dtype_counts = df.dtypes.value_counts()
                dtype_df = pd.DataFrame({
                    "Data Type": dtype_counts.index.astype(str),
                    "Count": dtype_counts.values
                })
                st.dataframe(dtype_df, use_container_width=True, hide_index=True)
            
            with col2:
                st.markdown("### 🔢 Missing Values")
                missing = df.isnull().sum()
                missing_df = pd.DataFrame({
                    "Column": missing.index,
                    "Missing": missing.values,
                    "Percentage": (missing.values / len(df) * 100).round(2)
                })
                missing_df = missing_df[missing_df["Missing"] > 0].sort_values("Missing", ascending=False)
                
                if len(missing_df) > 0:
                    st.dataframe(missing_df, use_container_width=True, hide_index=True)
                else:
                    st.success("No missing values!")
            
            st.markdown("---")
            
            # Export options
            st.markdown("### Export Data\")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"{selected_table_name}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                # Note: Excel export would require openpyxl
                st.button(
                    "Download as Excel",
                    disabled=True,
                    use_container_width=True,
                    help="Install openpyxl to enable"
                )
            
            with col3:
                st.button(
                    "Copy to Clipboard",
                    disabled=True,
                    use_container_width=True,
                    help="Copy selected data"
                )
        
        else:
            st.error("❌ Failed to load table data")


if __name__ == "__main__":
    show()
