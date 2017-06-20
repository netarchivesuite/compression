/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IFileTriLongLoaderImpl implements IFileTriLongLoader {
    @Override
    public IFileEntryMap getIFileEntryMap(String filename) throws FileNotFoundException {
        try {
            return new IFileEntryMap(filename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
