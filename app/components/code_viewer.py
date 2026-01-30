import streamlit as st
from pathlib import Path

def show_code_reference(file_path: str, start_line: int = None, end_line: int = None, description: str = ""):
    """
    Display source code reference in an expandable section.
    
    Parameters:
    -----------
    file_path : str
        Path to the source file (can be absolute or relative from project root)
    start_line : int, optional
        Starting line number (1-indexed)
    end_line : int, optional
        Ending line number (1-indexed)
    description : str
        Description of what this code does
    """
    try:
        path = Path(file_path)
        
        # If path is not absolute or doesn't exist, try resolving from project root
        if not path.is_absolute() or not path.exists():
            # Get project root (parent of app directory)
            app_dir = Path(__file__).parent.parent
            project_root = app_dir.parent
            path = project_root / file_path
        
        with st.expander(f"🔍 View Source Code: {path.name}"):
            if description:
                st.info(description)
            
            # Read file content
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Extract specific lines if specified
            if start_line is not None and end_line is not None:
                # Convert to 0-indexed
                code_snippet = ''.join(lines[start_line-1:end_line])
                st.code(code_snippet, language='python', line_numbers=True)
                st.caption(f"Lines {start_line}-{end_line} from `{path.name}`")
            else:
                code_snippet = ''.join(lines)
                st.code(code_snippet, language='python', line_numbers=True)
                st.caption(f"Full file: `{path.name}`")
            
            # Add file reference link (show relative path)
            st.caption(f"📁 File: `{file_path}`")
            
    except FileNotFoundError:
        st.error(f"Source file not found: {file_path}")
    except Exception as e:
        st.error(f"Error reading source file: {e}")


def show_sql_query(query: str, description: str = ""):
    """
    Display SQL query in an expandable section.
    
    Parameters:
    -----------
    query : str
        SQL query string
    description : str
        Description of what this query does
    """
    with st.expander("📊 View SQL Query"):
        if description:
            st.info(description)
        st.code(query, language='sql')


def show_optimization_highlight(title: str, before: str, after: str, impact: str):
    """
    Display optimization technique in a highlighted box.
    
    Parameters:
    -----------
    title : str
        Optimization technique name
    before : str
        Before state
    after : str
        After state
    impact : str
        Impact/improvement achieved
    """
    st.markdown(f"#### ⚡ {title}")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Before:**")
        st.text(before)
    with col2:
        st.markdown("**After:**")
        st.text(after)
    with col3:
        st.markdown("**Impact:**")
        st.success(impact)
