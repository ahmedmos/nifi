package org.apache.nifi.processors.azure.storage.utils;

import java.io.Serializable;

public class BlobInfo implements Comparable<BlobInfo>, Serializable, ListableEntity {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String primaryUri;
    private final String secondaryUri;
    private final String contentType;
    private final String contentLanguage;
    private final String etag;
    private final long lastModifiedTime;
    private final long length;
    private final String blobType;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getPrimaryUri() {
        return primaryUri;
    }

    public String getSecondaryUri() {
        return secondaryUri;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentLanguage() {
        return contentLanguage;
    }

    public String getEtag() {
        return etag;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public long getLength() {
        return length;
    }

    public String getBlobType() {
        return blobType;
    }

    public static final class Builder {
        private String name;
        private String primaryUri;
        private String secondaryUri;
        private String contentType;
        private String contentLanguage;
        private String etag;
        private long lastModifiedTime;
        private long length;
        private String blobType;
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder primaryUri(String primaryUri) {
            this.primaryUri = primaryUri;
            return this;
        }

        public Builder secondaryUri(String secondaryUri) {
            this.secondaryUri = secondaryUri;
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder contentLanguage(String contentLanguage) {
            this.contentLanguage = contentLanguage;
            return this;
        }

        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public Builder lastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder blobType(String blobType) {
            this.blobType = blobType;
            return this;
        }

        public BlobInfo build() {
            return new BlobInfo(this);
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((etag == null) ? 0 : etag.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BlobInfo other = (BlobInfo) obj;
        if (etag == null) {
            if (other.etag != null) {
                return false;
            }
        } else if (!etag.equals(other.etag)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(BlobInfo o) {
        return etag.compareTo(o.etag);
    }

    protected BlobInfo(final Builder builder) {
        this.name = builder.name;
        this.primaryUri = builder.primaryUri;
        this.secondaryUri = builder.secondaryUri;
        this.contentType = builder.contentType;
        this.contentLanguage = builder.contentLanguage;
        this.etag = builder.etag;
        this.lastModifiedTime = builder.lastModifiedTime;
        this.length = builder.length;
        this.blobType = builder.blobType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getIdentifier() {
        return getPrimaryUri();
    }

    @Override
    public long getTimestamp() {
        return getLastModifiedTime();
    }
}