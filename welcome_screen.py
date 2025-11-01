# welcome_screen.py - CORRECTED for Image Loading Paths and Recursive Destroy Call

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from PIL import Image, ImageTk
import webbrowser
from datetime import datetime
import os
import sys

class WelcomeScreen(tk.Frame):
    def __init__(self, master, on_file_selected_callback, base_path):
        super().__init__(master, bg="white")
        
        self.pack(fill='both', expand=True) 

        self.on_file_selected_callback = on_file_selected_callback
        self.master.configure(bg="white") 

        # --- Load Images ---
        try:
            # CORRECTED PATHS FOR IMAGES
            # When PyInstaller bundles, assets are placed in a folder relative to the executable.
            # base_path will be sys._MEIPASS for --onefile, or the dist/CubeApp/ folder for --onedir.
            # In both cases, assets are inside the 'assets' subfolder.

            # Construct the full absolute path to each image file
            bg_image_path = os.path.join(base_path, "assets", "ux.bmp")
            logo_image_path = os.path.join(base_path, "assets", "logo.png")
            import_icon_image_path = os.path.join(base_path, "assets", "import_icon.png")

            self.bg_image_raw = Image.open(bg_image_path)
            self.logo_image_raw = Image.open(logo_image_path)
            self.import_icon_image_raw = Image.open(import_icon_image_path)

            self.logo_photo = ImageTk.PhotoImage(self.logo_image_raw.resize((250, 80), Image.Resampling.LANCZOS))
            self.import_icon_photo = ImageTk.PhotoImage(self.import_icon_image_raw.resize((80, 80), Image.Resampling.LANCZOS))
            
        except Exception as e:
            # Improved error message to guide troubleshooting
            messagebox.showerror(
                "Image Load Error", 
                f"Could not load images. Please ensure 'ux.bmp', 'logo.png', and 'import_icon.png' are in the 'assets' subdirectory relative to the executable.\n"
                f"Attempted path: {os.path.join(base_path, 'assets', 'ux.bmp')}\n" # Show the attempted path
                f"Error: {str(e)}"
            )
            # When an image load error occurs, we want to shut down the application cleanly.
            # Calling self.master.destroy() will destroy the root window, which is appropriate.
            self.master.destroy() # This will close the application
            return # Exit the __init__ method after calling destroy to prevent further setup

        # --- Build UI elements for WelcomeScreen ---
        self.nav_frame = tk.Frame(self, bg='#2c3e50', height=40)
        self.nav_frame.pack(fill='x', side='top')

        self.about_button = tk.Button(
            self.nav_frame, 
            text="About", 
            bg='#2c3e50', 
            fg='white', 
            padx=10, 
            font=('Arial', 10, 'bold'),
            bd=0, 
            command=self._open_securemeters_website 
        )
        self.about_button.pack(side='left', padx=(10,0)) 
        self.about_button.bind("<Enter>", lambda e: self.about_button.config(bg='#34495e'))
        self.about_button.bind("<Leave>", lambda e: self.about_button.config(bg='#2c3e50'))
        
        self.info_button_welcome = tk.Button(
            self.nav_frame, 
            text="Info", 
            bg='#2c3e50', 
            fg='white', 
            padx=10, 
            font=('Arial', 10, 'bold'),
            bd=0, 
            command=self._show_info_dialog_welcome 
        )
        self.info_button_welcome.pack(side='left', padx=(10,0)) 
        self.info_button_welcome.bind("<Enter>", lambda e: self.info_button_welcome.config(bg='#34495e'))
        self.info_button_welcome.bind("<Leave>", lambda e: self.info_button_welcome.config(bg='#2c3e50'))
        
        self.main_canvas = tk.Canvas(self, bg="white", highlightthickness=0)
        self.main_canvas.pack(fill="both", expand=True)

        self.canvas_logo_item = self.main_canvas.create_image(
            0, 0, image=self.logo_photo, anchor="nw" 
        )

        self.canvas_import_button_item = self.main_canvas.create_image(
            0, 0, image=self.import_icon_photo, anchor="nw" 
        )
        self.main_canvas.tag_bind(self.canvas_import_button_item, "<Button-1>", self._on_import_button_click_canvas)
        self.main_canvas.tag_bind(self.canvas_import_button_item, "<Enter>", self._on_import_button_enter)
        self.main_canvas.tag_bind(self.canvas_import_button_item, "<Leave>", self._on_import_button_leave)
        
        self.ribbon = tk.Frame(self, bg='#1e2635', height=20)
        self.ribbon.pack(fill='x', side='bottom')

        self.main_canvas.bind("<Configure>", self.resize_elements)
        
        self.bg_photo = None 

        self.master.update_idletasks() 
        self.resize_elements(None) 

    def resize_elements(self, event):
        canvas_w = self.main_canvas.winfo_width()
        canvas_h = self.main_canvas.winfo_height()

        if canvas_w == 0 or canvas_h == 0: 
            return

        img_w, img_h = self.bg_image_raw.size

        scale = max(canvas_w / img_w, canvas_h / img_h)
        new_size = (int(img_w * scale), int(img_h * scale))
        
        resized_bg = self.bg_image_raw.resize(new_size, Image.Resampling.LANCZOS)
        self.bg_photo = ImageTk.PhotoImage(resized_bg) 

        bg_x = (canvas_w - new_size[0]) / 2
        bg_y = (canvas_h - new_size[1]) / 2

        bg_y_shifted = bg_y - int(new_size[1] * 0.06) 

        self.main_canvas.delete("background_image_tag") 
        self.main_canvas.create_image(bg_x, bg_y_shifted, image=self.bg_photo, anchor="nw", tags="background_image_tag")
        self.main_canvas.tag_lower("background_image_tag") 

        logo_center_x = canvas_w / 2
        logo_center_y = canvas_h * 0.15 

        self.main_canvas.coords(self.canvas_logo_item, logo_center_x, logo_center_y)
        self.main_canvas.itemconfigure(self.canvas_logo_item, anchor="center") 

        import_button_center_x = canvas_w / 2
        import_button_center_y = canvas_h * 0.32 

        self.main_canvas.coords(self.canvas_import_button_item, import_button_center_x, import_button_center_y)
        self.main_canvas.itemconfigure(self.canvas_import_button_item, anchor="center") 

        self.main_canvas.tag_raise(self.canvas_logo_item)
        self.main_canvas.tag_raise(self.canvas_import_button_item)
    
    def _on_import_button_click_canvas(self, event):
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xls *.xlsx *.xlsb")])
        if file_path:
            self.on_file_selected_callback(file_path)

    def _on_import_button_enter(self, event):
        self.main_canvas.config(cursor="hand2")

    def _on_import_button_leave(self, event):
        self.main_canvas.config(cursor="")

    def _open_securemeters_website(self):
        """Opens the Secure Meters website in a web browser."""
        try:
            webbrowser.open_new_tab("https://www.securemeters.com/")
        except Exception as e:
            messagebox.showerror("Web Browser Error", f"Could not open the website:\n{e}")

    def _show_info_dialog_welcome(self):
        """Displays application details in a pop-up dialog."""
        info_message = (
            "🟦 CUBE – SR Management Tool\n"
            "Version: 1.0\n"
            "Developed by: Suvidh Mathur\n"
            "Internship Project – Secure Meters Ltd.\n"
            f"Department: Analytics & CSS Repair Center\n"
            f"Deployment Date: July 17, {datetime.now().year}\n" 
            "Contact: suvidhmathur25@gmail.com\n\n"
            "🔸 Built using Python (Tkinter, Pandas, Dash)\n"
            "🔸 Designed for internal use by CSS Repair Teams"
        )
        messagebox.showinfo("About CUBE", info_message)

    def destroy(self):
        # This destroy method is for the WelcomeScreen frame itself
        # It should call the base class's destroy method, not itself recursively.
        # super().destroy() correctly calls the destroy method of the parent class (tk.Frame).
        try:
            self.pack_forget() # Hide the frame
            super().destroy() # Call the destroy method of the parent class (tk.Frame)
        except AttributeError:
            pass # If frame attribute doesn't exist (due to init error), just pass.
        

# For independent testing of this WelcomeScreen (optional, but useful during development)
if __name__ == "__main__":
    root = tk.Tk()
    root.title("Welcome Screen Test")
    root.geometry("900x600")

    def dummy_callback(path):
        print(f"File selected: {path}")
        messagebox.showinfo("File Selected", f"You chose: {path}. In a real app, this would switch views.")
        # To simulate destruction of WelcomeScreen and showing main dashboard:
        app.destroy() # This calls the WelcomeScreen's destroy method
        root.geometry("1400x850")
        root.title("Main Dashboard View")
        tk.Label(root, text="Main Dashboard Content Would Go Here!", font=('Arial', 20)).pack(pady=50)

    # When testing WelcomeScreen directly, sys._MEIPASS won't exist.
    # We provide a dummy base_path for testing purposes.
    if getattr(sys, 'frozen', False):
        test_base_path = sys._MEIPASS
    else:
        # In a development environment, 'assets' is likely in the same directory as welcome_screen.py
        test_base_path = os.path.abspath(os.path.dirname(__file__))

    # Pass test_base_path to WelcomeScreen when testing directly
    app = WelcomeScreen(root, dummy_callback, test_base_path)
    root.mainloop()