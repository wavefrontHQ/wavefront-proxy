describe('chain api test', () => {
    it('GET-list user', () => {
        cy.request({
            method: 'GET',
            url: 'https://nimba.wavefront.com/api/v2/proxy?offset=0&limit=100',
            headers: {
                Authorization: 'Bearer e5d76c15-f7f9-4dbe-b53d-0e67227877b1',
            }
        }).then((response) => {
            expect(response.status).equal(200)
            expect(response.body.response.items.map(i => i.name)).to.include('Proxy on proxy-dge')
        })
    })
})