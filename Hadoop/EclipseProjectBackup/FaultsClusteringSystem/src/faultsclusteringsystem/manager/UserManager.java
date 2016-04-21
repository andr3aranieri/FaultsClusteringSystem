package faultsclusteringsystem.manager;

import java.sql.SQLException;

import faultsclusteringsystem.db.UserDB;
import faultsclusteringsystem.entity.User;

public class UserManager {

	private UserDB userDB = new UserDB();
	
	public User getUser(int idUser) throws ClassNotFoundException, SQLException {
		return this.userDB.getUser(idUser);
	}
}
