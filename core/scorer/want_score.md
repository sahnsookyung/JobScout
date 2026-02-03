# Want score math

## Notation

- $N$ user wants, $M$ job facets, embedding dimension $d$.
- Want matrix $W \in \mathbb{R}^{N \times d}$ (row $i$ is want vector $w_i$).
- Facet matrix $F \in \mathbb{R}^{M \times d}$ (row $j$ is facet vector $f_j$).
- Facet weights $\alpha \in \mathbb{R}^M$ with $\alpha_j \ge 0$.

---

## 1) Cosine similarity matrix

Cosine similarity for two vectors $a,b \in \mathbb{R}^d$ is:

$$
\cos(a,b) = \frac{a \cdot b}{\lVert a \rVert_2 \, \lVert b \rVert_2}
$$

It lies in $[-1, 1]$. [web:31]

Define row-wise L2 normalization:

$$
\hat W_{i,:} = \frac{W_{i,:}}{\lVert W_{i,:} \rVert_2},
\qquad
\hat F_{j,:} = \frac{F_{j,:}}{\lVert F_{j,:} \rVert_2}
$$

After this normalization, cosine similarity equals a dot product, so the full pairwise cosine similarity matrix is:

$$
S = \hat W \hat F^{\mathsf T} \in \mathbb{R}^{N \times M}
$$

and each entry is:

$$
S_{ij} = \cos(w_i, f_j)
$$

This “normalize rows then multiply” is the standard way cosine similarity is computed in batch. [web:50]

---

## 2) Rescale to $[0,1]$

Map $S$ from $[-1,1]$ to $[0,1]$ (and clamp):

$$
N = \mathrm{clip}\left(\frac{S + 1}{2}, 0, 1\right)
$$

So every $N_{ij} \in [0,1]$. [web:36]

---

## 3) Best facet per want (row max)

“A want can match any facet” is modeled as a max over facets:

$$
b_i = \max_{j \in \{1,\dots,M\}} N_{ij}
$$

Vector form: $b = \max(N, \text{axis}=1) \in \mathbb{R}^{N}$.

---

## 4) Aggregate across wants (mean)

Unweighted aggregate similarity:

$$
A = \frac{1}{N}\sum_{i=1}^{N} b_i
$$

---

## 5) Facet-wise means and weighting

Compute the mean similarity per facet (column means):

$$
\mu_j = \frac{1}{N}\sum_{i=1}^{N} N_{ij}
\qquad\text{so}\qquad
\mu = \mathrm{mean}(N, \text{axis}=0) \in \mathbb{R}^{M}
$$

Then compute the facet-weighted score:

$$
W =
\begin{cases}
\frac{\alpha^{\mathsf T}\mu}{\mathbf{1}^{\mathsf T}\alpha}, & \text{if } \mathbf{1}^{\mathsf T}\alpha > 0 \\
A, & \text{otherwise}
\end{cases}
$$

Interpretation detail: this weights each facet by its *average similarity across the provided wants* ($\mu_j$), not only the facet that “won” the max for a want.

---

## 6) Final score (0–100)

Scale to a 0–100 score and cap at 100:

$$
\mathrm{WantScore} = \min(100, \, 100 \cdot W)
$$

---

## Missing wants / unmentioned facets

If the user only writes a few wants, then $N$ is small and all computations use only those wants.

But since $N_{ij}$ exists for every want–facet pair, facets the user didn’t explicitly mention can still influence the **weighted** score if their weights $\alpha_j$ are nonzero (via $\mu_j$).
