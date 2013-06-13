package edu.rutgers.cs541;

import java.awt.EventQueue;

/**
 * Contains the main() method and serves as the location where execution starts
 */
public class EntryPoint {
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		//using the AWT Event Queue makes sure its
		// executed on the GUI thread
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainWindow window = new MainWindow();
					window.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}
