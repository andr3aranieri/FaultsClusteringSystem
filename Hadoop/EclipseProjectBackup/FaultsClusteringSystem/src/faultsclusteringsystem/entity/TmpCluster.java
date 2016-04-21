package faultsclusteringsystem.entity;

public class TmpCluster {
	private Integer idTmpCluster;
	private Integer idUser;
	private Integer idDocument;
	private String description;
	private String center;
	
	public TmpCluster() {
	}

	public Integer getIdTmpCluster() {
		return idTmpCluster;
	}

	public void setIdTmpCluster(Integer idTmpCluster) {
		this.idTmpCluster = idTmpCluster;
	}

	public Integer getIdUser() {
		return idUser;
	}

	public void setIdUser(Integer idUser) {
		this.idUser = idUser;
	}

	public Integer getIdDocument() {
		return idDocument;
	}

	public void setIdDocument(Integer idDocument) {
		this.idDocument = idDocument;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getCenter() {
		return center;
	}

	public void setCenter(String center) {
		this.center = center;
	}
}
