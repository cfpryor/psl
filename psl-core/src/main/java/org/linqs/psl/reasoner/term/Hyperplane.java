/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2020 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.linqs.psl.reasoner.term;

import org.linqs.psl.util.ArrayUtils;

import java.lang.reflect.Array;

/**
 * Information representing a raw hyperplane.
 */
public class Hyperplane<E extends ReasonerLocalAtom> {
    private E[] variables;
    private float[] coefficients;
    private int size;
    private float constant;

    @SuppressWarnings("unchecked")
    public Hyperplane(Class<E> localVariableClass, int maxSize, float constant) {
        this((E[])Array.newInstance(localVariableClass, maxSize), new float[maxSize], constant, 0);
    }

    public Hyperplane(E[] variables, float[] coefficients, float constant, int index) {
        this.variables = variables;
        this.coefficients = coefficients;
        this.constant = constant;
        this.size = index;
    }

    public void addVariable(E variable, float coefficient) {
        variables[size] = variable;
        coefficients[size] = coefficient;
        size++;
    }

    public int size() {
        return size;
    }


    public E getVariable(int index) {
        if (index >= this.size) {
            throw new IndexOutOfBoundsException("Tried to access atom at index " + index + ", but only " + this.size + " exist.");
        }

        return variables[index];
    }


    public float getCoefficient(int index) {
        if (index >= this.size) {
            throw new IndexOutOfBoundsException("Tried to access coefficient at index " + index + ", but only " + this.size + " exist.");
        }

        return coefficients[index];
    }

    public void appendCoefficient(int index, float value) {
        if (index >= this.size) {
            throw new IndexOutOfBoundsException("Tried to access coefficient at index " + index + ", but only " + this.size + " exist.");
        }

        coefficients[index] += value;
    }

    public float getConstant() {
        return constant;
    }

    public void setConstant(float constant) {
        this.constant = constant;
    }

    public int indexOfVariable(E needle) {
        return ArrayUtils.indexOf(variables, size, needle);
    }

    public E[] getVariables() {
        return variables;
    }

    public float[] getCoefficients() {
        return coefficients;
    }
}
