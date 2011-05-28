package alien.commands;

import java.util.Date;

public class AlienTime {

	public static String getStamp() {

		Date date = new Date();
		return date.getMonth() + " " + date.getDay() + " " + date.getHours()
				+ ":" + date.getMinutes() + ":" + date.getSeconds() + "	jAuthen_v0.2		";
	}
}
