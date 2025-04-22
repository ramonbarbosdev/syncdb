package br.syncdb.DTO;

public class ConexaoDTO
{
    private CloudConnection cloud;
    private LocalConnection local;
    
   public CloudConnection getCloud() {
       return cloud;
   }
   public void setCloud(CloudConnection cloud) {
       this.cloud = cloud;
   }
   public LocalConnection getLocal() {
       return local;
   }
   public void setLocal(LocalConnection local) {
       this.local = local;
   }

    public static class CloudConnection
    {
        private String db_cloud_host;
        private String db_cloud_port;
        private String db_cloud_user;
        private String db_cloud_password;
        // getters e setters

       public String getDb_cloud_host() {
           return db_cloud_host;
       }
       public void setDb_cloud_host(String db_cloud_host) {
           this.db_cloud_host = db_cloud_host;
       }
       public String getDb_cloud_password() {
           return db_cloud_password;
       }
       public void setDb_cloud_password(String db_cloud_password) {
           this.db_cloud_password = db_cloud_password;
       }
       public String getDb_cloud_port() {
           return db_cloud_port;
       }
       public void setDb_cloud_port(String db_cloud_port) {
           this.db_cloud_port = db_cloud_port;
       }
       public String getDb_cloud_user() {
           return db_cloud_user;
       }
       public void setDb_cloud_user(String db_cloud_user) {
           this.db_cloud_user = db_cloud_user;
       }
    }

    public static class LocalConnection
    {
        private String db_local_host;
        private String db_local_port;
        private String db_local_user;
        private String db_local_password;

        public String getDb_local_host() {
            return db_local_host;
        }
        public void setDb_local_host(String db_local_host) {
            this.db_local_host = db_local_host;
        }
        public String getDb_local_password() {
            return db_local_password;
        }
        public void setDb_local_password(String db_local_password) {
            this.db_local_password = db_local_password;
        }
        public String getDb_local_port() {
            return db_local_port;
        }
        public void setDb_local_port(String db_local_port) {
            this.db_local_port = db_local_port;
        }
        public String getDb_local_user() {
            return db_local_user;
        }
        public void setDb_local_user(String db_local_user) {
            this.db_local_user = db_local_user;
        }
        
       
    }
}
