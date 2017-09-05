/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.adapter;


import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The AdapterManager maintains a map of lists of AdapterFactories keyed by target class.
 * This is exposed as a Singleton instance and must only be used by Oak internally.
 */
public class AdapterManager  {


    private static AdapterManager adapterManagerSingleton = new AdapterManager();

    public static AdapterManager getInstance() {
        return adapterManagerSingleton;
    }

    private AdapterManager() {
    }





    private Map<String, List<AdapterFactory>> adapterFactories = new ConcurrentHashMap<String, List<AdapterFactory>>();

    public <T> T adaptTo(@Nonnull  Object o, @Nonnull Class<T> targetClass) {
        List<AdapterFactory> possibleAdapters = adapterFactories.get(targetClass.getName());
        if ( possibleAdapters != null) {
            for (AdapterFactory af : possibleAdapters) {
                T target = af.adaptTo(o, targetClass);
                if (target != null) {
                    return target;
                }
            }
        }
        return null;
    }



    public synchronized void addAdapterFactory(AdapterFactory adapterFactory) {
        for( String className: adapterFactory.getTargetClasses()) {
            // create a new copy every time so that the read only usages don't have
            // to be synchronised.
            List<AdapterFactory> updatedAdapters = copyOrCreateAdapterFactoryList(className);
            updatedAdapters.add(adapterFactory);
            Collections.sort(updatedAdapters, new Comparator<AdapterFactory>() {
                @Override
                public int compare(AdapterFactory o1, AdapterFactory o2) {
                    return o1.getPriority() - o2.getPriority();
                }
            });
            adapterFactories.put(className, updatedAdapters);
        }
    }


    public synchronized void removeAdapterFactory(AdapterFactory adapterFactory) {
        for( String className: adapterFactory.getTargetClasses()) {
            // create a new copy every time so that the read only usages don't have
            // to be synchronised.
            List<AdapterFactory> updatedAdapters = copyOrCreateAdapterFactoryList(className);
            if (updatedAdapters != null) {
                updatedAdapters.remove(adapterFactory);
            }
            adapterFactories.put(className, updatedAdapters);
        }
    }

    private List<AdapterFactory> copyOrCreateAdapterFactoryList(String className) {
        List<AdapterFactory> copy = new ArrayList<AdapterFactory>();
        List<AdapterFactory> source = adapterFactories.get(className);
        if ( source != null) {
            copy.addAll(source);
        }
        return copy;
    }

}
