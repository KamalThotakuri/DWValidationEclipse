/**
 * 
 */
package com.acxiom.pmp.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kthota
 *
 */
public class DWConfiguration {
	private static Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger(DWConfiguration.class);
	
	public static void loadProps(String file) {
		File propsFile = new File(file);
		
		try {
			props.load(new FileInputStream(propsFile));
		} catch (FileNotFoundException e) {
			log.error("Config file './config/dwconfig.properties' not found, make sure its relative to the directory and try again");
		} catch (IOException e) {
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while reading config file './config/dwconfig.properties' with message "+e.getMessage());
			log.error(exTrace);
		}
	}

	public static Properties getProps() {
		return props;
	}

	public static void setProps(Properties props) {
		DWConfiguration.props = props;
	}
}
