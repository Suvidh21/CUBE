# welcome_screen.py
import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk

class WelcomeScreen(tk.Frame):
    def __init__(self, master, on_file_selected_callback):
        super().__init__(master, bg="white")
        self.pack(fill='both', expand=True) # This frame will fill the root window

        self.on_file_selected_callback = on_file_selected_callback
        self.master.title("Imports Dashboard") # Set initial window title
        self.master.geometry("1000x700") # A reasonable default size for the welcome screen
        self.master.configure(bg="white")

        # Load Images
        try:
            # Adjust paths if necessary. Assume images are in a 'assets' folder
            # Or directly in the same directory as welcome_screen.py
            self.bg_image_raw = Image.open("ux.bmp")  # Background BMP
            self.logo_image_raw = Image.open("logo.png").resize((250, 80), Image.Resampling.LANCZOS)
            self.logo_photo = ImageTk.PhotoImage(self.logo_image_raw)

            self.import_icon_image_raw = Image.open("import_icon.png").resize((80, 80), Image.Resampling.LANCZOS)
            self.import_icon_photo = ImageTk.PhotoImage(self.import_icon_image_raw)
        except Exception as e:
            messagebox.showerror("Image Load Error", f"Could not load images. Please ensure 'ux.bmp', 'logo.png', and 'import_icon.png' are in the correct directory.\nError: {str(e)}")
            self.master.destroy()
            return

        # Top Navigation Bar (Container)
        nav_frame = tk.Frame(self, bg='#2c3e50', height=40)
        nav_frame.pack(fill='x', side='top')

        nav_items = ['Contacts', 'Conversations', 'Marketing', 'Sales', 'Service', 'Automation', 'Reports']
        for item in nav_items:
            nav_label = tk.Label(nav_frame, text=item, bg='#2c3e50', fg='white', padx=10, font=('Arial', 10, 'bold'))
            nav_label.pack(side='left')
            # Add hover effects
            nav_label.bind("<Enter>", lambda e, l=nav_label: l.config(bg='#34495e'))
            nav_label.bind("<Leave>", lambda e, l=nav_label: l.config(bg='#2c3e50'))

        # Right-aligned icons
        tk.Label(nav_frame, text='⚙', bg='#2c3e50', fg='white', font=('Arial', 12)).pack(side='right', padx=10)
        tk.Label(nav_frame, text='👤', bg='#2c3e50', fg='white', font=('Arial', 12)).pack(side='right', padx=10)

        # Company Logo
        # Place logo in the main content area (self) after nav_frame,
        # but before the background image logic is bound.
        # Use a slightly darker color for the logo's background to make it float
        # or consider if the logo should be on the nav_frame itself.
        # For now, let's keep it simply packed.
        self.logo_label = tk.Label(self, image=self.logo_photo, bg='white')
        self.logo_label.pack(pady=(10, 5)) # Pack it first

        # Main Content Frame for background image and import button
        # This frame will handle the background image resizing
        self.content_frame = tk.Frame(self, bg='white')
        self.content_frame.pack(fill='both', expand=True)

        self.bg_label = tk.Label(self.content_frame) # This label holds the background image
        self.bg_label.place(x=0, y=0, relwidth=1, relheight=1) # Fills its parent frame

        # Import Button
        self.import_button = tk.Button(self.content_frame, image=self.import_icon_photo, bd=0,
                                         command=self._on_import_button_click, cursor='hand2',
                                         bg='white', activebackground='white')
        self.import_button.place(relx=0.5, rely=0.40, anchor='center') # Position relative to content_frame

        # Bottom Ribbon
        ribbon = tk.Frame(self, bg='#1e2635', height=20)
        ribbon.pack(fill='x', side='bottom')

        # Bind resize event to the content_frame
        self.content_frame.bind("<Configure>", self.resize_bg)
        self.bg_photo = None # To hold the PhotoImage instance

        # Call resize_bg once initially to set the background
        self.update_idletasks() # Ensure sizes are calculated before initial resize
        self.resize_bg(None) # Call with None event for initial setup

    def resize_bg(self, event):
        # Use self.content_frame's dimensions
        frame_w = self.content_frame.winfo_width()
        frame_h = self.content_frame.winfo_height()

        if frame_w == 0 or frame_h == 0: # Avoid division by zero on initial calls
            return

        img_w, img_h = self.bg_image_raw.size

        # Scale to cover the entire frame (aspect ratio preserved)
        scale = max(frame_w / img_w, frame_h / img_h)
        new_size = (int(img_w * scale), int(img_h * scale))
        
        resized_bg = self.bg_image_raw.resize(new_size, Image.Resampling.LANCZOS)
        self.bg_photo = ImageTk.PhotoImage(resized_bg) # Update the PhotoImage instance

        # Adjust y_offset if the image itself needs to be shifted within the label
        # This centers the background image within the available space after scaling
        x_offset = (frame_w - new_size[0]) / 2
        y_offset = (frame_h - new_size[1]) / 2
        
        # Apply a slight upward shift as per your original code's intention
        # This moves the background content up, potentially cropping the bottom.
        y_offset_for_image_position = -int(new_size[1] * 0.06) 

        self.bg_label.place(x=x_offset, y=y_offset + y_offset_for_image_position, width=new_size[0], height=new_size[1])
        self.bg_label.config(image=self.bg_photo)


    def _on_import_button_click(self):
        file_path = tk.filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsb *.xlsx *.xls")])
        if file_path:
            self.on_file_selected_callback(file_path)

# You can test this WelcomeScreen independently for development:
if __name__ == "__main__":
    root = tk.Tk()
    def dummy_callback(path):
        print(f"File selected in WelcomeScreen: {path}")
        messagebox.showinfo("File Selected", f"File: {path}")
        root.destroy() # Close after selection for test

    app = WelcomeScreen(root, dummy_callback)
    root.mainloop()