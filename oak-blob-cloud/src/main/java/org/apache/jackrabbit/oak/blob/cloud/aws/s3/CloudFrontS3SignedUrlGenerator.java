/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import com.amazonaws.services.cloudfront.CloudFrontUrlSigner;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Date;
import java.util.Properties;


/**
 *
 * To generate keys in PKCS8 format use OpenSSL
 * openssl genrsa -out private_key.pem 1024
 * openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in private_key.pem -out private_key.pkcs8
 * See http://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-trusted-signers.html#private-content-creating-cloudfront-key-pairs for
 * details on how to configure CloudFront.
 */
public class CloudFrontS3SignedUrlGenerator {

    public static final String CLOUD_FRONT_URL = "cloudFrontUrl";
    public static final String TTL = "ttl";
    public static final String PRIVATE_KEY = "privateKey";
    public static final String KEY_PAIR_ID = "keyPairId";
    public static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\n";
    public static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

    private static final Logger LOGGER = LoggerFactory.getLogger(CloudFrontS3SignedUrlGenerator.class);
    private String cloudFrontUrl;
    private long ttl;
    private String keyPairId;
    private RSAPrivateKey privateKey;




    public CloudFrontS3SignedUrlGenerator(Properties properties) throws InvalidKeySpecException, NoSuchAlgorithmException {
        this.cloudFrontUrl = (String) properties.get(CLOUD_FRONT_URL);
        this.ttl = Long.parseLong((String) properties.get(TTL));
        this.privateKey = getPrivateKey((String) properties.get(PRIVATE_KEY));
        this.keyPairId = (String) properties.get(KEY_PAIR_ID);

    }

    public URI getCloudFrontURI(String s3Key) {
        try {
            // could get the cloudFrontUrl, keyParId and private key based on the resource
            // so that multiple S3 stores or even multiple CDNs could be used, but for this PoC keeping it simple.
            return new URI(signS3Url(s3Key, ttl, cloudFrontUrl, keyPairId, privateKey));
        } catch (Exception e) {
            LOGGER.error("Unable to get or sign content identity",e);
        }
        return null;

    }




    @Nonnull
    private String signS3Url(@Nonnull String s3Key, long ttl, @Nonnull String cloudFrontUrl,
                             @Nonnull String keyPairId, @Nonnull RSAPrivateKey privateKey) throws InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {

        long expiry = (System.currentTimeMillis()/1000)+ttl;
        StringBuilder urlToSign = new StringBuilder();

        urlToSign.append(cloudFrontUrl)
                .append(s3Key);
        return CloudFrontUrlSigner.getSignedURLWithCannedPolicy(urlToSign.toString(), keyPairId, privateKey, new Date(expiry));
    }


    private RSAPrivateKey getPrivateKey(String privateKeyPKCS8) throws NoSuchAlgorithmException, InvalidKeySpecException {
        int is = privateKeyPKCS8.indexOf(BEGIN_PRIVATE_KEY);
        int ie = privateKeyPKCS8.indexOf(END_PRIVATE_KEY);
        if (ie < 0 || is < 0) {
            throw new IllegalArgumentException("Private Key is not correctly encoded, need a PEM encoded key with " +
                    "-----BEGIN PRIVATE KEY----- headers to indicate PKCS8 encoding.");
        }
        privateKeyPKCS8 = privateKeyPKCS8.substring(is+BEGIN_PRIVATE_KEY.length(),ie).trim();
        byte[] privateKeyBytes = Base64.decodeBase64(privateKeyPKCS8);

        // load the private key
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        KeySpec ks = new PKCS8EncodedKeySpec(privateKeyBytes);
        return (RSAPrivateKey) keyFactory.generatePrivate(ks);

    }

}
