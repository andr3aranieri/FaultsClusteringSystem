package faultsclusteringsystem.entity;
// Generated Jul 12, 2015 5:54:20 PM by Hibernate Tools 4.3.1

import java.util.Date;

/**
 * Stream generated by hbm2java
 */
public class Stream implements java.io.Serializable {

	private Integer idStream;
	private Integer idUser;
	private Date creationDate;
	private String type;
	private String parameters;
	private Boolean active;

	public Stream() {
	}

	public Integer getIdStream() {
		return this.idStream;
	}

	public void setIdStream(Integer idStream) {
		this.idStream = idStream;
	}

	public Integer getIdUser() {
		return idUser;
	}

	public void setIdUser(Integer idUser) {
		this.idUser = idUser;
	}

	public Date getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getParameters() {
		return this.parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public Boolean getActive() {
		return this.active;
	}

	public void setActive(Boolean active) {
		this.active = active;
	}

}
