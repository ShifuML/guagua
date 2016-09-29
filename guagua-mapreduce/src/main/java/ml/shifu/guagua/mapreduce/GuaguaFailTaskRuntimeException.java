/*
 * Copyright [2013-2014] PayPal Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.mapreduce;

/**
 * FailTaskRuntimeException which is used to fail hadoop mapper task.
 */
public class GuaguaFailTaskRuntimeException extends RuntimeException {


    private static final long serialVersionUID = 279485512893373010L;

    /**
     * Constructs a new fail task runtime exception with <code>null</code> as its detail message. The cause is
     * not
     * initialized, and may subsequently be initialized by a call to {@link #initCause}.
     */
    public GuaguaFailTaskRuntimeException() {
    }

    /**
     * Constructs a new fail task runtime exception with the specified detail message. The cause is not
     * initialized, and may subsequently be initialized by a call to {@link #initCause}.
     * 
     * @param message
     *            the detail message. The detail message is saved for later retrieval by the {@link #getMessage()}
     *            method.
     */
    public GuaguaFailTaskRuntimeException(String message) {
        super(message);
    }

    /**
     * Constructs a new fail task runtime exception with the specified detail message and cause.
     * <p>
     * Note that the detail message associated with <code>cause</code> is <i>not</i> automatically incorporated in this
     * runtime exception's detail message.
     * 
     * @param message
     *            the detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     * @param cause
     *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A <tt>null</tt>
     *            value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public GuaguaFailTaskRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new fail task runtime exception with the specified cause and a detail message of
     * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the class and detail
     * message of <tt>cause</tt>). This constructor is useful for runtime exceptions that are little more than wrappers
     * for other throwables.
     * 
     * @param cause
     *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A <tt>null</tt>
     *            value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public GuaguaFailTaskRuntimeException(Throwable cause) {
        super(cause);
    }

}
